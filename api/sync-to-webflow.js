import {createClient} from '@sanity/client'

// Sanity client
const sanityClient = createClient({
  projectId: 'b8bczekj',
  dataset: 'production',
  useCdn: false,
  apiVersion: '2023-01-01',
  token: process.env.SANITY_API_TOKEN
})

// Webflow collection IDs
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

// Store mapping of Sanity IDs to Webflow IDs (in production, use database)
const idMappings = {
  materialType: new Map(),
  material: new Map(),
  finish: new Map(),
  medium: new Map(),
  category: new Map(),
  location: new Map(),
  creator: new Map()
}

// Helper functions
function mapBilingualName(sanityItem) {
  return {
    'name-english': sanityItem.name?.en || '',
    'name-german': sanityItem.name?.de || '',
    name: sanityItem.name?.en || sanityItem.name?.de || 'Untitled',
    slug: sanityItem.slug?.current || generateSlug(sanityItem.name?.en || sanityItem.name?.de)
  }
}

function mapBilingualDescription(sanityItem) {
  return {
    'description-english': sanityItem.description?.en || '',
    'description-german': sanityItem.description?.de || ''
  }
}

function generateSlug(text) {
  if (!text) return 'untitled'
  return text.toLowerCase()
    .replace(/[^a-z0-9]/gi, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
}

// Webflow API helper
async function webflowRequest(endpoint, options = {}) {
  const baseUrl = 'https://api.webflow.com/v2'
  const response = await fetch(`${baseUrl}${endpoint}`, {
    headers: {
      'Authorization': `Bearer ${process.env.WEBFLOW_API_TOKEN}`,
      'Content-Type': 'application/json',
      ...options.headers
    },
    ...options
  })
  
  if (!response.ok) {
    const error = await response.text()
    throw new Error(`Webflow API error: ${response.status} ${error}`)
  }
  
  return response.json()
}

// Create items in Webflow
async function createWebflowItems(collectionId, items) {
  const batchSize = 50
  const results = []
  
  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize)
    console.log(`Creating batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(items.length/batchSize)} (${batch.length} items)`)
    
    try {
      const result = await webflowRequest(`/collections/${collectionId}/items`, {
        method: 'POST',
        body: JSON.stringify({ items: batch })
      })
      results.push(...result.items)
    } catch (error) {
      console.error(`Batch failed:`, error.message)
      throw error
    }
  }
  
  return results
}

// Delete items from Webflow
async function deleteWebflowItems(collectionId, itemIds) {
  const results = []
  
  for (const itemId of itemIds) {
    try {
      await webflowRequest(`/collections/${collectionId}/items/${itemId}`, {
        method: 'DELETE'
      })
      results.push({ itemId, status: 'deleted' })
    } catch (error) {
      console.error(`Failed to delete ${itemId}:`, error.message)
      results.push({ itemId, status: 'error', error: error.message })
    }
  }
  
  return results
}

// Get current Webflow items for comparison
async function getWebflowItems(collectionId) {
  try {
    const result = await webflowRequest(`/collections/${collectionId}/items?limit=100`)
    return result.items || []
  } catch (error) {
    console.error(`Failed to get Webflow items:`, error.message)
    return []
  }
}

// PHASE 1: Sync Material Types
async function syncMaterialTypes() {
  console.log('📋 Syncing Material Types...')
  
  const sanityData = await sanityClient.fetch(`
    *[_type == "materialType"] | order(sortOrder asc, name.en asc) {
      _id,
      name,
      description,
      sortOrder,
      slug
    }
  `)
  
  const webflowItems = sanityData.map(item => ({
    fieldData: {
      ...mapBilingualName(item),
      ...mapBilingualDescription(item),
      'sort-order': item.sortOrder || 0
    }
  }))
  
  // Create in Webflow
  const results = await createWebflowItems(WEBFLOW_COLLECTIONS.materialType, webflowItems)
  
  // Store mappings
  results.forEach((webflowItem, index) => {
    const sanityItem = sanityData[index]
    idMappings.materialType.set(sanityItem._id, webflowItem.id)
  })
  
  console.log(`✅ Material Types: ${results.length} created`)
  return results.length
}

// PHASE 2: Sync Finishes
async function syncFinishes() {
  console.log('🎨 Syncing Finishes...')
  
  const sanityData = await sanityClient.fetch(`
    *[_type == "finish"] | order(name.en asc) {
      _id,
      name,
      description,
      slug
    }
  `)
  
  const webflowItems = sanityData.map(item => ({
    fieldData: {
      ...mapBilingualName(item),
      ...mapBilingualDescription(item)
    }
  }))
  
  const results = await createWebflowItems(WEBFLOW_COLLECTIONS.finish, webflowItems)
  
  // Store mappings
  results.forEach((webflowItem, index) => {
    const sanityItem = sanityData[index]
    idMappings.finish.set(sanityItem._id, webflowItem.id)
  })
  
  console.log(`✅ Finishes: ${results.length} created`)
  return results.length
}

// PHASE 3: Sync Materials (with Material Type references)
async function syncMaterials() {
  console.log('🪨 Syncing Materials...')
  
  const sanityData = await sanityClient.fetch(`
    *[_type == "material"] | order(name.en asc) {
      _id,
      name,
      description,
      materialType->{_id, name},
      slug
    }
  `)
  
  const webflowItems = sanityData.map(item => ({
    fieldData: {
      ...mapBilingualName(item),
      ...mapBilingualDescription(item),
      'material-type': item.materialType?._id ? idMappings.materialType.get(item.materialType._id) : null
    }
  }))
  
  const results = await createWebflowItems(WEBFLOW_COLLECTIONS.material, webflowItems)
  
  // Store mappings
  results.forEach((webflowItem, index) => {
    const sanityItem = sanityData[index]
    idMappings.material.set(sanityItem._id, webflowItem.id)
  })
  
  console.log(`✅ Materials: ${results.length} created`)
  return results.length
}

// PHASE 4: Sync other collections
async function syncMediums() {
  console.log('🎭 Syncing Mediums...')
  
  const sanityData = await sanityClient.fetch(`
    *[_type == "medium"] | order(name.en asc) {
      _id,
      name,
      description,
      slug
    }
  `)
  
  const webflowItems = sanityData.map(item => ({
    fieldData: {
      ...mapBilingualName(item),
      ...mapBilingualDescription(item)
    }
  }))
  
  const results = await createWebflowItems(WEBFLOW_COLLECTIONS.medium, webflowItems)
  
  // Store mappings
  results.forEach((webflowItem, index) => {
    const sanityItem = sanityData[index]
    idMappings.medium.set(sanityItem._id, webflowItem.id)
  })
  
  console.log(`✅ Mediums: ${results.length} created`)
  return results.length
}

// Main sync function
async function performCompleteSync() {
  const startTime = Date.now()
  let totalSynced = 0
  
  try {
    console.log('🚀 Starting Complete Sanity → Webflow Sync')
    console.log('='.repeat(60))
    
    // Clear existing mappings
    Object.values(idMappings).forEach(map => map.clear())
    
    // Phase 1: Foundation data (no dependencies)
    console.log('\n📋 PHASE 1: Foundation Data')
    totalSynced += await syncMaterialTypes()
    totalSynced += await syncFinishes()
    
    // Phase 2: Reference data (with dependencies)
    console.log('\n🔗 PHASE 2: Reference Data')
    totalSynced += await syncMaterials()
    totalSynced += await syncMediums()
    
    // TODO: Add more phases for categories, locations, creators, artworks
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(1)
    console.log(`\n✅ Complete sync finished in ${duration}s`)
    console.log(`📊 Total items synced: ${totalSynced}`)
    
    return {
      success: true,
      totalSynced,
      duration: `${duration}s`,
      timestamp: new Date().toISOString()
    }
    
  } catch (error) {
    console.error('❌ Sync failed:', error.message)
    throw error
  }
}

// Main API handler
export default async function handler(req, res) {
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
    // Verify required environment variables
    if (!process.env.SANITY_API_TOKEN) {
      throw new Error('SANITY_API_TOKEN environment variable is required')
    }
    if (!process.env.WEBFLOW_API_TOKEN) {
      throw new Error('WEBFLOW_API_TOKEN environment variable is required')
    }
    
    console.log('🔔 Sync triggered via API')
    const result = await performCompleteSync()
    
    res.status(200).json({
      message: 'Sync completed successfully',
      ...result
    })
    
  } catch (error) {
    console.error('API Error:', error.message)
    res.status(500).json({
      error: 'Sync failed',
      message: error.message,
      timestamp: new Date().toISOString()
    })
  }
} 