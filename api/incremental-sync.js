const {createClient} = require('@sanity/client')
const crypto = require('crypto')

// Sanity client
const sanityClient = createClient({
  projectId: 'b8bczekj',
  dataset: 'production',
  useCdn: false,
  apiVersion: '2023-01-01',
  token: process.env.SANITY_API_TOKEN
})

// Webflow configuration
const WEBFLOW_SITE_ID = '68664367794a916bfa6d247c'
const WEBFLOW_COLLECTIONS = {
  materialType: '6873884cedcec21fab8dd8dc',
  material: '687388627483ef982c64eb3f',
  finish: '6873886339818fe4cd550b03',
  medium: '686e55eace746485413c6bfb',
  category: '686e4fd904ae9f54468f85df',
  location: '686e4ff7977797cc67e99b97',
  creator: '686e4d544cb3505ce3b1412c',
  artwork: '686e50ba1170cab27bfa6c49'
}

// Sync state tracking
class SyncStateManager {
  constructor() {
    this.lastSyncTimestamps = new Map()
    this.itemMappings = new Map() // Sanity ID -> Webflow ID
    this.syncProgress = {
      phase: '',
      current: 0,
      total: 0,
      errors: []
    }
  }

  async loadState() {
    try {
      // Load sync state from Sanity
      const result = await sanityClient.fetch(`
        *[_type == "webflowSyncSettings" && _id == "sync-state"][0] {
          lastSyncTimestamps,
          itemMappings,
          lastUpdated
        }
      `)
      
      if (result?.lastSyncTimestamps) {
        this.lastSyncTimestamps = new Map(Object.entries(JSON.parse(result.lastSyncTimestamps)))
        console.log(`📅 Loaded sync timestamps for ${this.lastSyncTimestamps.size} collections`)
      }
      
      if (result?.itemMappings) {
        this.itemMappings = new Map(Object.entries(JSON.parse(result.itemMappings)))
        console.log(`🔗 Loaded ${this.itemMappings.size} item mappings`)
      }
      
    } catch (error) {
      console.log('📅 Starting fresh sync state:', error.message)
    }
  }

  async saveState() {
    try {
      await sanityClient.createOrReplace({
        _type: 'webflowSyncSettings',
        _id: 'sync-state',
        lastSyncTimestamps: JSON.stringify(Object.fromEntries(this.lastSyncTimestamps)),
        itemMappings: JSON.stringify(Object.fromEntries(this.itemMappings)),
        lastUpdated: new Date().toISOString()
      })
      
      console.log('💾 Sync state saved successfully')
    } catch (error) {
      console.error('❌ Failed to save sync state:', error.message)
    }
  }

  getLastSyncTime(collection) {
    return this.lastSyncTimestamps.get(collection) || '1970-01-01T00:00:00Z'
  }

  setLastSyncTime(collection) {
    this.lastSyncTimestamps.set(collection, new Date().toISOString())
  }

  addMapping(sanityId, webflowId) {
    this.itemMappings.set(sanityId, webflowId)
  }

  getWebflowId(sanityId) {
    return this.itemMappings.get(sanityId)
  }
}

// Robust Webflow API client with comprehensive error handling
class WebflowClient {
  constructor() {
    this.baseUrl = 'https://api.webflow.com/v2'
    this.requestDelay = 1000 // Start with 1 second delay
    this.maxRetries = 5
  }

  async request(endpoint, options = {}, retryCount = 0) {
    const url = `${this.baseUrl}${endpoint}`
    
    try {
      const response = await fetch(url, {
        headers: {
          'Authorization': `Bearer ${process.env.WEBFLOW_API_TOKEN}`,
          'Content-Type': 'application/json',
          ...options.headers
        },
        ...options
      })

      // Handle rate limits with exponential backoff
      if (response.status === 429) {
        const retryAfter = response.headers.get('retry-after') || Math.pow(2, retryCount + 2)
        const waitTime = parseInt(retryAfter) * 1000
        
        if (retryCount < this.maxRetries) {
          console.log(`⏳ Rate limited, waiting ${waitTime/1000}s (attempt ${retryCount + 1}/${this.maxRetries})`)
          await this.delay(waitTime)
          return this.request(endpoint, options, retryCount + 1)
        } else {
          throw new Error(`Rate limit exceeded after ${this.maxRetries} retries`)
        }
      }

      if (!response.ok) {
        const errorText = await response.text()
        throw new Error(`Webflow API error ${response.status}: ${errorText}`)
      }

      // Add delay between successful requests to avoid rate limits
      await this.delay(this.requestDelay)
      
      return response.json()
      
    } catch (error) {
      if (retryCount < this.maxRetries && !error.message.includes('Rate limit exceeded')) {
        console.log(`🔄 Retrying request (attempt ${retryCount + 1}/${this.maxRetries}): ${error.message}`)
        await this.delay(Math.pow(2, retryCount + 1) * 1000)
        return this.request(endpoint, options, retryCount + 1)
      }
      throw error
    }
  }

  async delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
  }

  async getCollectionItems(collectionId) {
    const items = []
    let offset = 0
    const limit = 100

    while (true) {
      const result = await this.request(`/collections/${collectionId}/items?limit=${limit}&offset=${offset}`)
      const batch = result.items || []
      items.push(...batch)
      
      if (batch.length < limit) break
      offset += limit
    }

    return items
  }

  async createItem(collectionId, itemData) {
    return this.request(`/collections/${collectionId}/items`, {
      method: 'POST',
      body: JSON.stringify({ items: [itemData] })
    })
  }

  async updateItem(collectionId, itemId, itemData) {
    return this.request(`/collections/${collectionId}/items/${itemId}`, {
      method: 'PATCH',
      body: JSON.stringify({ items: [{ id: itemId, ...itemData }] })
    })
  }

  async deleteItem(collectionId, itemId) {
    return this.request(`/collections/${collectionId}/items/${itemId}`, {
      method: 'DELETE'
    })
  }
}

// Collection synchronizers with incremental logic
class CollectionSynchronizer {
  constructor(webflowClient, stateManager) {
    this.webflow = webflowClient
    this.state = stateManager
  }

  async syncCollection(collectionType, collectionId, mapFieldsFunction) {
    console.log(`🔄 Starting incremental sync for ${collectionType}...`)
    
    const lastSync = this.state.getLastSyncTime(collectionType)
    console.log(`📅 Last synced: ${lastSync}`)

    // Get changed items from Sanity since last sync
    const changedSanityItems = await this.getChangedSanityItems(collectionType, lastSync)
    console.log(`📝 Found ${changedSanityItems.length} changed items in Sanity`)

    // Get current Webflow items
    const webflowItems = await this.webflow.getCollectionItems(collectionId)
    console.log(`🌐 Found ${webflowItems.length} existing items in Webflow`)

    // Build mapping of existing items
    const webflowBySlug = new Map()
    webflowItems.forEach(item => {
      if (item.fieldData?.slug) {
        webflowBySlug.set(item.fieldData.slug, item)
      }
    })

    let created = 0, updated = 0, errors = 0

    // Process each changed item
    for (const sanityItem of changedSanityItems) {
      try {
        const fieldData = mapFieldsFunction(sanityItem)
        const slug = fieldData.slug
        const existingItem = webflowBySlug.get(slug)

        if (existingItem) {
          // Update existing item
          await this.webflow.updateItem(collectionId, existingItem.id, { fieldData })
          this.state.addMapping(sanityItem._id, existingItem.id)
          updated++
          console.log(`  ✅ Updated ${slug}`)
        } else {
          // Create new item
          const result = await this.webflow.createItem(collectionId, { fieldData })
          if (result.items && result.items[0]) {
            this.state.addMapping(sanityItem._id, result.items[0].id)
            created++
            console.log(`  ✅ Created ${slug}`)
          }
        }

      } catch (error) {
        console.error(`  ❌ Failed to sync ${sanityItem.name || sanityItem._id}: ${error.message}`)
        errors++
      }
    }

    // Update sync timestamp
    this.state.setLastSyncTime(collectionType)
    
    console.log(`✅ ${collectionType}: ${created} created, ${updated} updated, ${errors} errors`)
    return { created, updated, errors }
  }

  async getChangedSanityItems(collectionType, since) {
    const queries = {
      materialType: `*[_type == "materialType" && _updatedAt > "${since}"] | order(_updatedAt asc) { _id, _updatedAt, name, description, sortOrder, slug }`,
      material: `*[_type == "material" && _updatedAt > "${since}"] | order(_updatedAt asc) { _id, _updatedAt, name, description, materialType->{_id}, slug }`,
      finish: `*[_type == "finish" && _updatedAt > "${since}"] | order(_updatedAt asc) { _id, _updatedAt, name, description, slug }`,
      medium: `*[_type == "medium" && _updatedAt > "${since}"] | order(_updatedAt asc) { _id, _updatedAt, name, description, slug }`,
      category: `*[_type == "category" && _updatedAt > "${since}"] | order(_updatedAt asc) { _id, _updatedAt, title, description, slug }`,
      location: `*[_type == "location" && _updatedAt > "${since}"] | order(_updatedAt asc) { _id, _updatedAt, name, type, address, city->{name}, country->{name}, website, times, phone, email, description, slug }`,
      creator: `*[_type == "creator" && _updatedAt > "${since}"] | order(_updatedAt asc) { _id, _updatedAt, name, biography, portrait, nationality, specialties, slug }`,
      artwork: `*[_type == "artwork" && _updatedAt > "${since}"] | order(_updatedAt asc) { _id, _updatedAt, name, workTitle, description, creator->{_id}, category->{_id}, materials[]->{_id}, medium[]->{_id}, finishes[]->{_id}, size, year, price, slug, images[]{asset->{_id, url, originalFilename}, alt} }`
    }

    const query = queries[collectionType]
    if (!query) {
      throw new Error(`Unknown collection type: ${collectionType}`)
    }

    return sanityClient.fetch(query)
  }
}

// Field mapping functions (same as before but cleaner)
const FieldMappers = {
  materialType: (item) => ({
    'name-english': item.name?.en || '',
    'name-german': item.name?.de || '',
    'description-english': item.description?.en || '',
    'description-german': item.description?.de || '',
    name: item.name?.en || item.name?.de || 'Untitled',
    slug: item.slug?.current || generateSlug(item.name?.en || item.name?.de),
    'sort-order': item.sortOrder || 0
  }),

  material: (item) => ({
    'name-english': item.name?.en || '',
    'name-german': item.name?.de || '',
    'description-english': item.description?.en || '',
    'description-german': item.description?.de || '',
    name: item.name?.en || item.name?.de || 'Untitled',
    slug: item.slug?.current || generateSlug(item.name?.en || item.name?.de)
  }),

  category: (item) => ({
    name: item.title?.en || item.title?.de || 'Untitled',
    slug: item.slug?.current || generateSlug(item.title?.en || item.title?.de),
    'name-english': item.title?.en || '',
    'name-german': item.title?.de || '',
    'description-english': item.description?.en || '',
    'description-german': item.description?.de || ''
  }),

  // Add other mappers as needed...
}

function generateSlug(text) {
  if (!text) return 'untitled'
  return text.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '')
}

// Main incremental sync function
async function performIncrementalSync(progressCallback = null) {
  const startTime = Date.now()
  let totalStats = { created: 0, updated: 0, errors: 0 }

  try {
    console.log('🚀 Starting Incremental Sanity → Webflow Sync')
    console.log('='.repeat(50))

    // Verify environment
    if (!process.env.SANITY_API_TOKEN || !process.env.WEBFLOW_API_TOKEN) {
      throw new Error('Missing required environment variables')
    }

    // Initialize components
    const webflowClient = new WebflowClient()
    const stateManager = new SyncStateManager()
    const synchronizer = new CollectionSynchronizer(webflowClient, stateManager)

    // Load existing sync state
    await stateManager.loadState()

    // Sync collections in dependency order
    const syncOrder = [
      { type: 'materialType', id: WEBFLOW_COLLECTIONS.materialType, mapper: FieldMappers.materialType },
      { type: 'category', id: WEBFLOW_COLLECTIONS.category, mapper: FieldMappers.category },
      // Add others as we build them out
    ]

    for (const { type, id, mapper } of syncOrder) {
      if (progressCallback) {
        progressCallback({ phase: type, message: `Syncing ${type}...` })
      }

      const stats = await synchronizer.syncCollection(type, id, mapper)
      totalStats.created += stats.created
      totalStats.updated += stats.updated
      totalStats.errors += stats.errors
    }

    // Save final state
    await stateManager.saveState()

    const duration = ((Date.now() - startTime) / 1000).toFixed(1)
    console.log(`\n✅ Incremental sync completed in ${duration}s`)
    console.log(`📊 Results: ${totalStats.created} created, ${totalStats.updated} updated, ${totalStats.errors} errors`)

    return {
      success: true,
      duration: `${duration}s`,
      stats: totalStats,
      timestamp: new Date().toISOString()
    }

  } catch (error) {
    console.error('❌ Incremental sync failed:', error.message)
    throw error
  }
}

// Export for API handler
module.exports = async function handler(req, res) {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*')
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization')
  
  if (req.method === 'OPTIONS') {
    return res.status(200).end()
  }
  
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' })
  }

  try {
    console.log('🔔 Incremental sync triggered via API')
    const result = await performIncrementalSync()
    
    res.status(200).json({
      message: 'Incremental sync completed successfully',
      ...result
    })
    
  } catch (error) {
    console.error('API Error:', error.message)
    res.status(500).json({
      error: 'Incremental sync failed',
      message: error.message,
      timestamp: new Date().toISOString()
    })
  }
} 