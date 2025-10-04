const fs = require('fs')
const path = require('path')

// Load environment variables manually
const envPath = path.join(__dirname, '..', '.env')
if (fs.existsSync(envPath)) {
  const envContent = fs.readFileSync(envPath, 'utf8')
  envContent.split('\n').forEach(line => {
    const [key, value] = line.split('=')
    if (key && value) {
      process.env[key.trim()] = value.trim()
    }
  })
}

let createClient
try {
  createClient = require('@sanity/client').createClient
} catch (e) {
  try {
    // Fallback to Sanity subproject dependency
    createClient = require(path.join(__dirname, '..', 'sanity-cms', 'node_modules', '@sanity', 'client')).createClient
    console.log('ℹ️  Using @sanity/client from sanity-cms/node_modules')
  } catch (e2) {
    throw new Error("@sanity/client not found. Install it at repo root or in sanity-cms.")
  }
}
const crypto = require('crypto')
const https = require('https')

// Sanity client
const sanityClient = createClient({
  projectId: 'b8bczekj',
  dataset: 'production',
  useCdn: false,
  apiVersion: '2023-01-01',
  token: process.env.SANITY_API_TOKEN
})

// Webflow site ID (from env)
const WEBFLOW_SITE_ID = process.env.WEBFLOW_SITE_ID
if (!WEBFLOW_SITE_ID) {
  throw new Error('WEBFLOW_SITE_ID environment variable is required')
}

// Webflow locale IDs (will be resolved at runtime)
let WEBFLOW_LOCALES = {
  en: null,      // Primary locale ID
  'de-DE': null  // German locale ID
}

// CLI args
const ARGS = process.argv.slice(2)
function getArg(name) {
  const pref = `--${name}=`
  const hit = ARGS.find(a => a.startsWith(pref))
  return hit ? hit.substring(pref.length) : null
}
const FLAG_QUICK = ARGS.includes('--quick')
const FLAG_CHECK_ONLY = ARGS.includes('--check-only')
const FLAG_PUBLISH = ARGS.includes('--publish')
const ARG_ONLY = getArg('only') // e.g. --only=creator|artwork|material
const ARG_ITEM = getArg('item') // e.g. --item=creator-id-123 (single item sync)

// Webflow collection IDs (resolved dynamically at runtime)
let WEBFLOW_COLLECTIONS = null

function normalize(str) {
  return (str || '')
    .toString()
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
}

async function resolveWebflowLocales() {
  try {
    const siteInfo = await webflowRequest(`/sites/${WEBFLOW_SITE_ID}`)
    if (siteInfo.locales) {
      // Primary locale
      if (siteInfo.locales.primary?.cmsLocaleId) {
        WEBFLOW_LOCALES.en = siteInfo.locales.primary.cmsLocaleId
        console.log(`  🌍 Primary locale (en): ${WEBFLOW_LOCALES.en}`)
      }
      // Secondary locales
      if (Array.isArray(siteInfo.locales.secondary)) {
        const germanLocale = siteInfo.locales.secondary.find(l => l.tag === 'de' || l.tag === 'de-DE')
        if (germanLocale?.cmsLocaleId) {
          WEBFLOW_LOCALES['de-DE'] = germanLocale.cmsLocaleId
          console.log(`  🌍 German locale (de-DE): ${WEBFLOW_LOCALES['de-DE']}`)
        }
      }
    }
  } catch (e) {
    console.warn('⚠️  Failed to resolve locales:', e.message)
  }
}

async function resolveWebflowCollections() {
  // Allow explicit override via env JSON
  if (process.env.WEBFLOW_COLLECTIONS_JSON) {
    try {
      const parsed = JSON.parse(process.env.WEBFLOW_COLLECTIONS_JSON)
      return parsed
    } catch (e) {
      console.warn('⚠️  Invalid WEBFLOW_COLLECTIONS_JSON, falling back to API resolution')
    }
  }

  const siteCollections = await webflowRequest(`/sites/${WEBFLOW_SITE_ID}/collections`)
  const bySlug = new Map()
  for (const c of (siteCollections.collections || siteCollections || [])) {
    // Support both possible response shapes
    const slug = normalize(c.slug || c.displayName || c.singularName)
    bySlug.set(slug, c)
  }

  // Helper to find a collection by possible names
  const pick = (...candidates) => {
    for (const name of candidates) {
      const c = bySlug.get(normalize(name))
      if (c) return c.id
    }
    return null
  }

  const resolved = {
    materialType: pick('material-types', 'material type', 'material types', 'materialtype'),
    material: pick('materials', 'material'),
    finish: pick('finishes', 'finish'),
    medium: pick('mediums', 'media', 'medium'),
    category: pick('categories', 'category'),
    location: pick('locations', 'location'),
    creator: pick('creators', 'creator', 'profiles', 'profile'),
    artwork: pick('artworks', 'artwork', 'works', 'work')
  }

  // Validate presence
  const missing = Object.entries(resolved).filter(([, v]) => !v).map(([k]) => k)
  if (missing.length > 0) {
    console.warn(`⚠️  Missing collections on site: ${missing.join(', ')}`)
  }
  return resolved
}

// Store mapping of Sanity IDs to Webflow IDs (in production, use database)
const idMappings = {
  materialType: new Map(),
  material: new Map(),
  finish: new Map(),
  medium: new Map(),
  category: new Map(),
  location: new Map(),
  creator: new Map(),
  artwork: new Map()
}

// Persistent ID mappings system (like asset mappings)
let persistentIdMappings = new Map()
let persistentHashes = new Map() // key: collection:sanityId => lastSyncedHash

// Load ID mappings from Sanity
async function loadIdMappings() {
  try {
    const result = await sanityClient.fetch(`
      *[_type == "webflowSyncSettings" && _id == "id-mappings"][0] {
        idMappings
      }
    `)
    
    if (result?.idMappings) {
      const parsedMappings = JSON.parse(result.idMappings)
      persistentIdMappings = new Map(Object.entries(parsedMappings))
      console.log(`🔗 Loaded ${persistentIdMappings.size} ID mappings from Sanity`)
    } else {
      console.log('🔗 No existing ID mappings found, starting fresh')
      persistentIdMappings = new Map()
    }
  } catch (error) {
    console.log('🔗 Failed to load ID mappings, starting fresh:', error.message)
    persistentIdMappings = new Map()
  }

  // Load hashes
  try {
    const result2 = await sanityClient.fetch(`
      *[_type == "webflowSyncSettings" && _id == "sync-hashes"][0] {
        hashes
      }
    `)
    if (result2?.hashes) {
      const parsed = JSON.parse(result2.hashes)
      persistentHashes = new Map(Object.entries(parsed))
      console.log(`🔗 Loaded ${persistentHashes.size} item hashes`)
    } else {
      persistentHashes = new Map()
    }
  } catch (e) {
    console.log('🔗 Failed to load hashes, starting fresh:', e.message)
    persistentHashes = new Map()
  }
}

// Clear stale ID mappings when Webflow is actually empty
async function clearStaleIdMappings() {
  console.log('🧹 Clearing stale ID mappings from Sanity...')
  
  try {
    await sanityClient.createOrReplace({
      _type: 'webflowSyncSettings',
      _id: 'id-mappings',
      idMappings: JSON.stringify({}),
      lastUpdated: new Date().toISOString()
    })
    
    // Clear in-memory mappings too
    persistentIdMappings = new Map()
    Object.keys(idMappings).forEach(collection => {
      idMappings[collection].clear()
    })
    
    console.log('✅ Cleared all stale ID mappings')
  } catch (error) {
    console.error('❌ Failed to clear ID mappings:', error.message)
  }
}

// Save ID mappings to Sanity
async function saveIdMappings() {
  try {
    // Merge all collection mappings into one persistent store
    const allMappings = {}
    Object.entries(idMappings).forEach(([collection, map]) => {
      map.forEach((webflowId, sanityId) => {
        allMappings[`${collection}:${sanityId}`] = webflowId
      })
    })
    
    await sanityClient.createOrReplace({
      _type: 'webflowSyncSettings',
      _id: 'id-mappings',
      idMappings: JSON.stringify(allMappings),
      lastUpdated: new Date().toISOString()
    })
    
    console.log(`💾 Saved ${Object.keys(allMappings).length} ID mappings to Sanity`)
  } catch (error) {
    console.error('❌ Failed to save ID mappings:', error.message)
  }

  // Save hashes
  try {
    const allHashes = Object.fromEntries(persistentHashes)
    await sanityClient.createOrReplace({
      _type: 'webflowSyncSettings',
      _id: 'sync-hashes',
      hashes: JSON.stringify(allHashes),
      lastUpdated: new Date().toISOString()
    })
    console.log(`💾 Saved ${Object.keys(allHashes).length} item hashes to Sanity`)
  } catch (e) {
    console.error('❌ Failed to save hashes:', e.message)
  }
}

// Load persistent mappings into memory collections
function loadPersistentMappings() {
  Object.keys(idMappings).forEach(collection => {
    idMappings[collection].clear()
  })
  
  persistentIdMappings.forEach((webflowId, key) => {
    const [collection, sanityId] = key.split(':')
    if (idMappings[collection]) {
      idMappings[collection].set(sanityId, webflowId)
    }
  })
  
  console.log(`🔄 Loaded mappings into memory: ${Object.entries(idMappings).map(([k,v]) => `${k}:${v.size}`).join(', ')}`)
}

// Rebuild ID mappings from existing Webflow data (if mappings are empty)
async function rebuildIdMappings() {
  const collections = [
    { key: 'creator', id: WEBFLOW_COLLECTIONS.creator, sanityType: 'creator' },
    { key: 'artwork', id: WEBFLOW_COLLECTIONS.artwork, sanityType: 'artwork' },
    { key: 'category', id: WEBFLOW_COLLECTIONS.category, sanityType: 'category' },
    { key: 'material', id: WEBFLOW_COLLECTIONS.material, sanityType: 'material' },
    { key: 'medium', id: WEBFLOW_COLLECTIONS.medium, sanityType: 'medium' },
    { key: 'finish', id: WEBFLOW_COLLECTIONS.finish, sanityType: 'finish' },
    { key: 'materialType', id: WEBFLOW_COLLECTIONS.materialType, sanityType: 'materialType' },
    { key: 'location', id: WEBFLOW_COLLECTIONS.location, sanityType: 'location' }
  ]
  
  for (const collection of collections) {
    if (idMappings[collection.key].size === 0) {
      console.log(`🔄 Rebuilding ${collection.key} mappings...`)
      
      // Get existing Webflow items
      const webflowItems = await getWebflowItems(collection.id)
      
      // Get corresponding Sanity items
      const sanityItems = await sanityClient.fetch(`*[_type == "${collection.sanityType}"] { _id, slug, name }`)
      
      // Match by slug or name
      for (const webflowItem of webflowItems) {
        const slug = webflowItem.fieldData.slug
        const name = webflowItem.fieldData.name
        
        const sanityItem = sanityItems.find(item => 
          item.slug?.current === slug || 
          item.name === name ||
          generateSlug(item.name) === slug
        )
        
        if (sanityItem) {
          idMappings[collection.key].set(sanityItem._id, webflowItem.id)
        }
      }
      
      console.log(`  ✅ Rebuilt ${idMappings[collection.key].size} ${collection.key} mappings`)
    }
  }
}

// Find existing Webflow item by Sanity ID (using stored mappings)
function findExistingWebflowId(sanityId, collection) {
  return idMappings[collection]?.get(sanityId) || null
}

// Asset tracking system for incremental image sync (using Sanity as storage)
let assetMappings = new Map()

// Load asset mappings from Sanity
async function loadAssetMappings() {
  try {
    const result = await sanityClient.fetch(`
      *[_type == "webflowSyncSettings" && _id == "asset-mappings"][0] {
        assetMappings
      }
    `)
    
    if (result?.assetMappings) {
      const parsedMappings = JSON.parse(result.assetMappings)
      assetMappings = new Map(Object.entries(parsedMappings))
      console.log(`📁 Loaded ${assetMappings.size} asset mappings from Sanity`)
    } else {
      console.log('📁 No existing asset mappings found, starting fresh')
      assetMappings = new Map()
    }
  } catch (error) {
    console.log('📁 Failed to load asset mappings, starting fresh:', error.message)
    assetMappings = new Map()
  }
}

// Save asset mappings to Sanity
async function saveAssetMappings() {
  try {
    const mappings = Object.fromEntries(assetMappings)
    
    await sanityClient.createOrReplace({
      _type: 'webflowSyncSettings',
      _id: 'asset-mappings',
      assetMappings: JSON.stringify(mappings),
      lastUpdated: new Date().toISOString()
    })
    
    console.log(`💾 Saved ${assetMappings.size} asset mappings to Sanity`)
  } catch (error) {
    console.error('❌ Failed to save asset mappings:', error.message)
  }
}

// Check if image metadata has changed
function hasImageMetadataChanged(sanityImage, trackedAsset) {
  const currentAltText = sanityImage.alt?.en || sanityImage.alt?.de || ''
  const currentFilename = sanityImage.asset?.originalFilename || ''
  const currentUrl = sanityImage.asset?.url || ''
  
  return (
    trackedAsset.altText !== currentAltText ||
    trackedAsset.filename !== currentFilename ||
    trackedAsset.url !== currentUrl
  )
}

// Update image metadata in Webflow
async function updateImageMetadata(webflowAssetId, altText) {
  try {
    // Try the standard assets endpoint first
    await webflowRequest(`/sites/${WEBFLOW_SITE_ID}/assets/${webflowAssetId}`, {
      method: 'PATCH',
      body: JSON.stringify({
        altText: altText || ''
      })
    })
    console.log(`  🏷️  Updated alt text: ${altText}`)
    return true
  } catch (error) {
    // If that fails, try alternative format
    try {
      await webflowRequest(`/sites/${WEBFLOW_SITE_ID}/assets/${webflowAssetId}`, {
        method: 'PATCH',
        body: JSON.stringify({
          alt: altText || ''
        })
      })
      console.log(`  🏷️  Updated alt text (alt format): ${altText}`)
      return true
    } catch (error2) {
      console.warn(`  ⚠️  Failed to update alt text (tried both formats): ${error.message}`)
      return false
    }
  }
}

// Convert Sanity block content to Webflow's Rich Text JSON format
function convertSanityBlocksToWebflowRichText(blocks) {
  if (!blocks || !Array.isArray(blocks)) return null

  const content = blocks.map(block => {
    if (block._type === 'block' && block.children) {
      const paragraphContent = block.children.map(child => {
        const marks = child.marks?.map(mark => {
          if (mark === 'strong') return { type: 'bold' }
          if (mark === 'em') return { type: 'italic' }
          if (mark.startsWith('link-')) return { type: 'link', attrs: { href: mark.substring(5) } }
          return { type: mark }
        }).filter(Boolean)

        return {
          type: 'text',
          text: child.text || '',
          ...(marks && marks.length > 0 && { marks })
        }
      })
      
      return {
        type: 'paragraph',
        content: paragraphContent
      }
    }
    return null
  }).filter(Boolean)

  if (content.length === 0) return null
  
  return {
    type: 'doc',
    content: content
  }
}

// Extract plain text from Sanity rich text blocks (for non-rich-text fields)
function extractTextFromBlocks(blocks) {
  if (!blocks || !Array.isArray(blocks)) return ''
  
  return blocks
    .map(block => {
      if (block._type === 'block' && block.children) {
        return block.children
          .map(child => child.text || '')
          .join('')
      }
      return ''
    })
    .join(' ')
    .trim()
}

// Collection-specific mapping functions
function mapMaterialTypeFields(sanityItem, locale = 'en') {
  const isGerman = locale === 'de-DE' || locale === 'de'
  return {
    'sort-order': sanityItem.sortOrder || 0,
    name: isGerman ? (sanityItem.name?.de || sanityItem.name?.en || 'Untitled') : (sanityItem.name?.en || sanityItem.name?.de || 'Untitled'),
    slug: sanityItem.slug?.current || generateSlug(sanityItem.name?.en || sanityItem.name?.de)
  }
}

function mapCategoryFields(sanityItem, locale = 'en') {
  const isGerman = locale === 'de-DE' || locale === 'de'
  return {
    name: isGerman ? (sanityItem.title?.de || sanityItem.title?.en || 'Untitled') : (sanityItem.title?.en || sanityItem.title?.de || 'Untitled'),
    slug: sanityItem.slug?.current || generateSlug(sanityItem.title?.en || sanityItem.title?.de),
    description: isGerman ? (sanityItem.description?.de || '') : (sanityItem.description?.en || '')
  }
}

function mapCreatorFields(sanityItem, locale = 'en') {
  // Locale-agnostic fields (same for all locales)
  const baseFields = {
    'name': sanityItem.name || 'Untitled',
    'slug': sanityItem.slug?.current || generateSlug(sanityItem.name),
    'hero-image': (sanityItem.cover?.asset?.url ? {
      url: sanityItem.cover.asset.url,
      alt: (sanityItem.cover?.alt?.en || sanityItem.cover?.alt?.de || sanityItem.name || '')
    } : undefined),
    'profile-image': (sanityItem.image?.asset?.url ? {
      url: sanityItem.image.asset.url,
      alt: (sanityItem.image?.alt?.en || sanityItem.image?.alt?.de || sanityItem.name || '')
    } : undefined),
    'website': sanityItem.website || '',
    'email': sanityItem.email || '',
    'birth-year': sanityItem.birthYear ? parseInt(sanityItem.birthYear, 10) : null,
    'category': sanityItem.category?._ref ? idMappings.category.get(sanityItem.category._ref) || null : null
  }

  // Locale-specific fields
  const isGerman = locale === 'de-DE' || locale === 'de'
  const localeFields = {
    'biography': extractTextFromBlocks(isGerman ? sanityItem.biography?.de : sanityItem.biography?.en),
    'portrait-english': extractTextFromBlocks(isGerman ? sanityItem.portrait?.de : sanityItem.portrait?.en), // TODO: rename to 'portrait' once slug is fixed in Webflow
    'nationality': isGerman ? (sanityItem.nationality?.de || '') : (sanityItem.nationality?.en || ''),
    'specialties': isGerman ? (sanityItem.specialties?.de?.join(', ') || '') : (sanityItem.specialties?.en?.join(', ') || '')
  }

  return { ...baseFields, ...localeFields }
}

function mapLocationFields(sanityItem, locale = 'en') {
  const isGerman = locale === 'de-DE' || locale === 'de'
  return {
    name: isGerman ? (sanityItem.name?.de || sanityItem.name?.en || 'Untitled') : (sanityItem.name?.en || sanityItem.name?.de || 'Untitled'),
    slug: sanityItem.slug?.current || generateSlug(sanityItem.name?.en || sanityItem.name?.de),
    'location-type': mapLocationType(sanityItem.type),
    website: sanityItem.website || '',
    email: sanityItem.email || ''
  }
}

function mapMediumFinishFields(sanityItem, locale = 'en') {
  const isGerman = locale === 'de-DE' || locale === 'de'
  return {
    name: isGerman ? (sanityItem.name?.de || sanityItem.name?.en || 'Untitled') : (sanityItem.name?.en || sanityItem.name?.de || 'Untitled'),
    slug: sanityItem.slug?.current || generateSlug(sanityItem.name?.en || sanityItem.name?.de)
  }
}

function generateSlug(text) {
  if (!text) return 'untitled'
  return text.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '')
}

// Map Sanity location types to Webflow location types
function mapLocationType(sanityType) {
  const typeMapping = {
    'museum': 'Museum',
    'shop-gallery': 'Shop / Gallery',
    'studio': 'Studio'
  }
  return typeMapping[sanityType] || 'Shop / Gallery' // Default to Shop / Gallery
}

// Webflow API helper with rate limit handling
async function webflowRequest(endpoint, options = {}, retryCount = 0) {
  const baseUrl = 'https://api.webflow.com/v2'
  const maxRetries = 3
  
  const response = await fetch(`${baseUrl}${endpoint}`, {
    headers: {
      'Authorization': `Bearer ${process.env.WEBFLOW_API_TOKEN}`,
      'Content-Type': 'application/json',
      ...options.headers
    },
    ...options
  })
  
  // Handle rate limits with exponential backoff
  if (response.status === 429 && retryCount < maxRetries) {
    const waitTime = Math.pow(2, retryCount) * 1000 // 1s, 2s, 4s
    console.log(`⏳ Rate limited, waiting ${waitTime/1000}s before retry ${retryCount + 1}/${maxRetries}`)
    await new Promise(resolve => setTimeout(resolve, waitTime))
    return webflowRequest(endpoint, options, retryCount + 1)
  }
  
  if (!response.ok) {
    const errorBody = await response.text()
    console.error('Webflow API Error Response:', errorBody)
    throw new Error(`Webflow API error: ${response.status} ${errorBody}`)
  }
  
  // Handle empty responses for DELETE requests
  if (response.status === 204 || options.method === 'DELETE') {
    return {} // Return empty object for successful deletions
  }

  return response.json()
}

// Update one item in a Webflow collection (UPSERT path)
async function updateWebflowItem(collectionId, itemId, fieldData, localeId = null) {
  try {
    await sleep(300) // basic throttle
    const endpoint = localeId 
      ? `/collections/${collectionId}/items/${itemId}?cmsLocaleId=${localeId}`
      : `/collections/${collectionId}/items/${itemId}`
    const result = await webflowRequest(endpoint, {
      method: 'PATCH',
      body: JSON.stringify({ fieldData })
    })
    return result
  } catch (error) {
    console.error(`Update failed for ${itemId} (locale: ${localeId || 'primary'}):`, error.message)
    throw error
  }
}

// Update secondary locale (German) for an item
async function updateItemGermanLocale(collectionId, itemId, sanityItem, fieldMapper) {
  if (!WEBFLOW_LOCALES['de-DE']) {
    return // Skip if German locale not available
  }
  
  try {
    // For artworks with customImageSync, sanityItem might be a wrapped object
    const actualItem = sanityItem._sanityItem || sanityItem
    
    const germanFields = fieldMapper ? fieldMapper(actualItem, 'de-DE') : {}
    
    // For artworks without standard fieldMapper, manually map German content
    if (!fieldMapper && actualItem.workTitle) {
      germanFields['work-title'] = actualItem.workTitle?.de || ''
      germanFields['description'] = actualItem.description?.de || ''
    }
    
    // Only send locale-specific fields, not base fields like name/slug/images
    const localeOnlyFields = {}
    const localeFieldNames = ['biography', 'portrait', 'portrait-english', 'nationality', 'specialties', 'description', 'work-title']
    localeFieldNames.forEach(field => {
      if (germanFields[field] !== undefined) {
        localeOnlyFields[field] = germanFields[field]
      }
    })
    
    if (Object.keys(localeOnlyFields).length > 0) {
      await updateWebflowItem(collectionId, itemId, localeOnlyFields, WEBFLOW_LOCALES['de-DE'])
      console.log(`    🇩🇪 Updated German locale`)
    }
  } catch (error) {
    console.warn(`    ⚠️  Failed to update German locale: ${error.message}`)
  }
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
      const created = Array.isArray(result?.items) ? result.items : []
      results.push(...created)
      // If --publish was requested, publish created items immediately
      if (FLAG_PUBLISH && created.length > 0) {
        const createdIds = created.map(i => i?.id).filter(Boolean)
        try {
          await publishWebflowItems(collectionId, createdIds)
        } catch (e) {
          console.warn(`  ⚠️  Failed to publish ${createdIds.length} created items: ${e.message}`)
        }
      }
    } catch (error) {
      console.error(`Batch failed:`, error.message)
      throw error
    }
  }
  
  return results
}

// Publish items in batches
async function publishWebflowItems(collectionId, itemIds) {
  const batchSize = 50
  for (let i = 0; i < itemIds.length; i += batchSize) {
    const batch = itemIds.slice(i, i + batchSize)
    let attempt = 0
    const maxAttempts = 3
    while (attempt < maxAttempts) {
      try {
        console.log(`  🚀 Publishing batch ${Math.floor(i/batchSize)+1}/${Math.ceil(itemIds.length/batchSize)} (${batch.length} items)...`)
        await webflowRequest(`/collections/${collectionId}/items/publish`, {
          method: 'POST',
          body: JSON.stringify({ itemIds: batch })
        })
        break
      } catch (e) {
        attempt++
        const wait = Math.pow(2, attempt) * 1000
        console.warn(`  ⚠️  Publish failed (attempt ${attempt}/${maxAttempts}): ${e.message}. Retrying in ${wait/1000}s...`)
        await sleep(wait)
      }
    }
  }
}

// Delete items from Webflow (with batch processing)
async function deleteWebflowItems(collectionId, itemIds) {
  const results = []
  const batchSize = 50 // Can use a larger batch size for logging, requests are sequential
  
  for (let i = 0; i < itemIds.length; i += batchSize) {
    const batch = itemIds.slice(i, i + batchSize)
    console.log(`  🗑️  Deleting batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(itemIds.length/batchSize)} (${batch.length} items)`)
    
    // Process items sequentially within the batch
    for (const itemId of batch) {
      let attempts = 0
      while (attempts < 3) {
        try {
          await webflowRequest(`/collections/${collectionId}/items/${itemId}`, {
            method: 'DELETE'
          })
          results.push({ itemId, status: 'deleted' })
          break // Success, exit while loop
        } catch (error) {
          attempts++
          if (error.message.includes('429') && attempts < 3) {
            const waitTime = attempts * 2000 // 2s, 4s
            console.log(`  ⏳ Rate limited, retrying ${itemId} in ${waitTime/1000}s...`)
            await new Promise(resolve => setTimeout(resolve, waitTime))
            continue
          }
          console.warn(`  ⚠️  Failed to delete ${itemId}: ${error.message}`)
          results.push({ itemId, status: 'error', error: error.message })
          break // Failure, exit while loop
        }
      }
    }
  }
  
  const successCount = results.filter(r => r.status === 'deleted').length
  const errorCount = results.filter(r => r.status === 'error').length
  
  if (errorCount > 0) {
    console.warn(`  ⚠️  ${errorCount} items failed to delete (${successCount} successful)`)
  }
  
  return results
}

// Get current Webflow items for comparison (with pagination)
async function getWebflowItems(collectionId) {
  try {
    let allItems = []
    let offset = 0
    const limit = 100
    
    while (true) {
      const result = await webflowRequest(`/collections/${collectionId}/items?limit=${limit}&offset=${offset}`)
      const items = result.items || []
      
      allItems.push(...items)
      
      // If we got fewer items than the limit, we've reached the end
      if (items.length < limit) {
        break
      }
      
      offset += limit
    }
    
    console.log(`  📄 Found ${allItems.length} existing items`)
    return allItems
  } catch (error) {
    console.error(`Failed to get Webflow items:`, error.message)
    return []
  }
}

// Clear existing items from a collection
async function clearWebflowCollection(collectionId, collectionName) {
  console.log(`  🧹 Clearing existing ${collectionName}...`)
  const existingItems = await getWebflowItems(collectionId)
  
  if (existingItems.length === 0) {
    console.log(`  ✅ No existing ${collectionName} to clear`)
    return
  }
  
  console.log(`  🗑️  Deleting ${existingItems.length} existing ${collectionName}`)
  const itemIds = existingItems.map(item => item.id)
  await deleteWebflowItems(collectionId, itemIds)
  console.log(`  ✅ Cleared ${existingItems.length} existing ${collectionName}`)
}

// Add after the existing utility functions
async function downloadImageBuffer(url) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error('Image download timeout (30s)'))
    }, 30000) // 30 second timeout
    
    const request = https.get(url, (response) => {
      if (response.statusCode !== 200) {
        clearTimeout(timeout)
        reject(new Error(`Failed to download image: ${response.statusCode}`))
        return
      }
      
      const chunks = []
      response.on('data', (chunk) => chunks.push(chunk))
      response.on('end', () => {
        clearTimeout(timeout)
        resolve(Buffer.concat(chunks))
      })
      response.on('error', (error) => {
        clearTimeout(timeout)
        reject(error)
      })
    })
    
    request.on('error', (error) => {
      clearTimeout(timeout)
      reject(error)
    })
    
    request.on('timeout', () => {
      clearTimeout(timeout)
      request.destroy()
      reject(new Error('Image download timeout'))
    })
  })
}

function generateMD5Hash(buffer) {
  return crypto.createHash('md5').update(buffer).digest('hex')
}

function hashObjectStable(obj) {
  const json = JSON.stringify(obj, Object.keys(obj).sort())
  return generateMD5Hash(Buffer.from(json))
}

function sleep(ms){ return new Promise(r=>setTimeout(r, ms)) }

async function uploadImageToWebflow(imageUrl, siteName, altText = null, originalFilename = null) {
  try {
    // Add a 5-second delay to respect asset creation rate limits
    await new Promise(resolve => setTimeout(resolve, 5000))

    // 1. Download image from Sanity
    console.log(`  📥 Downloading: ${imageUrl}`)
    const imageBuffer = await downloadImageBuffer(imageUrl)
    
    // 2. Generate MD5 hash
    const fileHash = generateMD5Hash(imageBuffer)
    
    // 3. Create meaningful filename from alt text or description
    let filename = originalFilename || 'artwork-image.jpg'
    
    if (altText && altText.trim()) {
      // Use alt text to create descriptive filename
      const cleanAltText = altText
        .trim()
        .substring(0, 80) // Limit length
        .replace(/[^a-zA-Z0-9\s\-_]/g, '') // Remove special chars
        .replace(/\s+/g, '-') // Replace spaces with dashes
        .toLowerCase()
      
      if (cleanAltText) {
        const extension = originalFilename ? 
          originalFilename.split('.').pop() : 'jpg'
        filename = `${cleanAltText}.${extension}`
      }
    }
    
    // 4. Create asset metadata in Webflow
    const metadataResponse = await webflowRequest(`/sites/${WEBFLOW_SITE_ID}/assets`, {
      method: 'POST',
      body: JSON.stringify({
        fileName: filename,
        fileHash: fileHash,
        fileSize: imageBuffer.length
      })
    })
    
    if (!metadataResponse.uploadUrl || !metadataResponse.uploadDetails) {
      throw new Error('Failed to get upload credentials from Webflow')
    }
    
    // 5. Upload to Amazon S3 using FormData (the correct way)
    console.log('  📤 Uploading to S3 with FormData...')
    
    const form = new FormData()
    
    // Add all presigned fields from uploadDetails
    Object.entries(metadataResponse.uploadDetails).forEach(([key, value]) => {
      form.append(key, value)
    })
    
    // Add the file last - create a Blob from the buffer for fetch()
    const blob = new Blob([imageBuffer], { 
      type: metadataResponse.uploadDetails['content-type'] || 'image/jpeg' 
    })
    form.append('file', blob, filename)
    
    const uploadResponse = await fetch(metadataResponse.uploadUrl, {
      method: 'POST',
      body: form
    })
    
    if (!uploadResponse.ok) {
      const errorText = await uploadResponse.text()
      throw new Error(`S3 upload failed: ${uploadResponse.status} - ${errorText}`)
    }
    
    // 6. Set alt text if provided (currently not working with Webflow API v2)
    // if (altText && metadataResponse.id) {
    //   await updateImageMetadata(metadataResponse.id, altText)
    // }
    
    // 7. Return Webflow asset ID
    console.log(`  ✅ Uploaded: ${filename}${altText ? ' (with alt text)' : ''}`)
    return metadataResponse.id
    
  } catch (error) {
    console.error(`❌ Failed to upload image. URL: ${imageUrl}, Error:`, error)
    return null
  }
}

async function syncArtworkImages(artworkImages) {
  if (!artworkImages || artworkImages.length === 0) {
    return []
  }
  
  console.log(`  🖼️  Syncing ${artworkImages.length} images with incremental logic...`)
  const webflowAssetIds = []
  let uploadedCount = 0
  let updatedCount = 0
  let skippedCount = 0
  
  for (const image of artworkImages) {
    if (!image.asset?.url) continue
    
    const sanityAssetId = image.asset._id
    const originalFilename = image.asset.originalFilename || ''
    const imageUrl = image.asset.url
    
    // Create enhanced alt text from artwork context
    const altText = image.alt?.en || image.alt?.de || ''
    const artworkContext = image.artworkContext
    
    let enhancedAltText = altText
    if (!enhancedAltText && artworkContext) {
      // Build descriptive text from artwork data
      const parts = []
      if (artworkContext.creatorName) parts.push(artworkContext.creatorName)
      if (artworkContext.workTitle) parts.push(artworkContext.workTitle)
      else if (artworkContext.name) parts.push(artworkContext.name)
      
      enhancedAltText = parts.join(' - ')
    }
    
    // Check if we've seen this image before
    const existingAsset = assetMappings.get(sanityAssetId)
    
    if (!existingAsset) {
      // New image - upload it
      console.log(`  📤 Uploading new image: ${originalFilename}`)
      console.log(`  🏷️  Enhanced description: ${enhancedAltText}`)
        
      const assetId = await uploadImageToWebflow(
        imageUrl, 
        'artwork', 
        enhancedAltText, 
        originalFilename
      )
      
      if (assetId) {
        // Track this asset
        assetMappings.set(sanityAssetId, {
          webflowAssetId: assetId,
          altText: enhancedAltText,
          filename: originalFilename,
          url: imageUrl,
          lastUpdated: new Date().toISOString()
        })
        
        webflowAssetIds.push(assetId)
        uploadedCount++
      }
    } else if (hasImageMetadataChanged({...image, alt: {en: enhancedAltText}}, existingAsset)) {
      // Metadata changed - update tracking (no API call needed)
      console.log(`  🔄 Updating metadata for: ${originalFilename}`)
      console.log(`  🏷️  New description: ${enhancedAltText}`)
      
      // Update tracking (skip API call since it doesn't work)
      assetMappings.set(sanityAssetId, {
        ...existingAsset,
        altText: enhancedAltText,
        filename: originalFilename,
        url: imageUrl,
        lastUpdated: new Date().toISOString()
      })
      updatedCount++
      
      webflowAssetIds.push(existingAsset.webflowAssetId)
    } else {
      // No change - skip
      console.log(`  ⏭️  Skipping unchanged image: ${originalFilename}`)
      webflowAssetIds.push(existingAsset.webflowAssetId)
      skippedCount++
    }
  }
  
  console.log(`  ✅ Image sync complete: ${uploadedCount} uploaded, ${updatedCount} updated, ${skippedCount} skipped`)
  return webflowAssetIds
}

// Universal duplicate-aware collection sync helper with UPSERT and delta detection
async function syncCollection(options) {
  const {
    name,
    collectionId,
    mappingKey,
    sanityQuery,
    fieldMapper,
    customImageSync = null,
    limit = null
  } = options
  
  console.log(`📋 Syncing ${name}...`)
  
  let sanityData = await sanityClient.fetch(sanityQuery)
  const dataArray = Array.isArray(sanityData) ? sanityData : (sanityData ? [sanityData] : [])
  
  if (limit && Number.isFinite(limit)) {
    const capped = Math.max(0, Number(limit))
    if (dataArray.length > capped) {
      console.log(`  🔎 Limiting to first ${capped} of ${dataArray.length} ${name}`)
      sanityData = dataArray.slice(0, capped)
    } else {
      sanityData = dataArray
    }
  } else {
    sanityData = dataArray
  }
  
  console.log(`  • Total Sanity items: ${sanityData.length}`)
  
  // Get existing Webflow items for adoption logic
  const existingWebflowItems = await getWebflowItems(collectionId)
  const webflowBySlug = new Map()
  for (const wfItem of existingWebflowItems) {
    const slug = wfItem?.fieldData?.slug
    if (slug) webflowBySlug.set(slug, wfItem)
  }
  
  // Process items and check for duplicates
  const newItems = []
  const updateItems = []
  let existingCount = 0
  
  for (const item of sanityData) {
    let existingId = idMappings[mappingKey].get(item._id) || item.webflowId || null
    
    // Prepare mapped fields (used for both create and update)
    let mappedFieldsForId
    try {
      if (customImageSync) {
        mappedFieldsForId = await customImageSync(item)
        mappedFieldsForId = mappedFieldsForId.fieldData
      } else {
        mappedFieldsForId = fieldMapper(item)
      }
    } catch (e) {
      console.warn(`  ⚠️  Field mapping failed for ${item._id}: ${e.message}`)
      continue
    }

    // Verify webflowId still exists, clear if stale
    if (existingId) {
      try {
        await webflowRequest(`/collections/${collectionId}/items/${existingId}`)
      } catch (error) {
        if (error.message.includes('404')) {
          console.log(`  ❌ Stale webflowId ${existingId} not found, clearing mapping`)
          idMappings[mappingKey].delete(item._id)
          const key = `${mappingKey}:${item._id}`
          persistentHashes.delete(key)
          existingId = null
        }
      }
    }

    // If no ID, try to adopt by slug
    if (!existingId && mappedFieldsForId?.slug) {
      const adopt = webflowBySlug.get(mappedFieldsForId.slug)
      if (adopt?.id) {
        existingId = adopt.id
        idMappings[mappingKey].set(item._id, existingId)
        console.log(`  ↳ Adopted existing item by slug for ${mappingKey}:${item._id} → ${existingId}`)
      }
    }

    if (!existingId) {
      // New item - prepare for creation
      const webflowItem = { fieldData: mappedFieldsForId }
      newItems.push({ item, webflowItem })
    } else {
      // Existing item - check if update is needed via delta hash
      const mapped = { ...mappedFieldsForId }
      delete mapped.slug // Don't change slug on update to avoid conflicts
      const webflowItem = { fieldData: mapped }
      const hash = hashObjectStable(webflowItem.fieldData)
      const key = `${mappingKey}:${item._id}`
      const prev = persistentHashes.get(key)
      
      if (prev !== hash) {
        updateItems.push({ item, webflowId: existingId, webflowItem, hash, key })
      }
      existingCount++
    }
  }
  
  console.log(`  📊 ${newItems.length} new, ${updateItems.length} to update, ${existingCount} existing`)
  
  // Create new items in Webflow (primary locale)
  let results = []
  if (newItems.length > 0) {
    results = await createWebflowItems(collectionId, newItems.map(ni => ni.webflowItem))
    
    // Store new mappings and update German locale
    for (let index = 0; index < results.length; index++) {
      const webflowItem = results[index]
      const sanityItem = newItems[index].item
      idMappings[mappingKey].set(sanityItem._id, webflowItem.id)
      // Store hash for newly created items
      const hash = hashObjectStable(webflowItem.fieldData)
      persistentHashes.set(`${mappingKey}:${sanityItem._id}`, hash)
      
      // Update German locale if fieldMapper supports it
      if (fieldMapper && webflowItem.id) {
        await updateItemGermanLocale(collectionId, webflowItem.id, sanityItem, fieldMapper)
      }
    }
  }
  
  // Update existing items
  let updatedCount = 0
  const updatedItemIds = []
  if (updateItems.length > 0) {
    console.log(`  🔄 Updating ${updateItems.length} existing ${name} items (delta only)...`)
  }
  for (let i = 0; i < updateItems.length; i++) {
    const u = updateItems[i]
    try {
      await updateWebflowItem(collectionId, u.webflowId, u.webflowItem.fieldData)
      persistentHashes.set(u.key, u.hash)
      updatedItemIds.push(u.webflowId)
      
      // Update German locale
      if (fieldMapper) {
        await updateItemGermanLocale(collectionId, u.webflowId, u.item, fieldMapper)
      }
      
      updatedCount++
      if ((i + 1) % 25 === 0 || i === updateItems.length - 1) {
        console.log(`    ↳ Updated ${i + 1}/${updateItems.length}`)
      }
    } catch (e) {
      if (String(e.message).includes('429')) {
        await sleep(1500)
        await updateWebflowItem(collectionId, u.webflowId, u.webflowItem.fieldData)
        if (fieldMapper) {
          await updateItemGermanLocale(collectionId, u.webflowId, u.item, fieldMapper)
        }
        persistentHashes.set(u.key, u.hash)
        updatedItemIds.push(u.webflowId)
        updatedCount++
      } else {
        console.warn(`  ⚠️  Update failed for ${u.webflowId}: ${e.message}`)
      }
    }
  }
  
  // Batch publish all updated items
  if (FLAG_PUBLISH && updatedItemIds.length > 0) {
    console.log(`  📢 Batch publishing ${updatedItemIds.length} updated items...`)
    await publishWebflowItems(collectionId, updatedItemIds)
  }

  const unchangedCount = Math.max(existingCount - updateItems.length, 0)
  console.log(`✅ ${name}: ${results.length} created, ${updatedCount} updated, ${unchangedCount} unchanged`)
  return results.length + updatedCount
}

// PHASE 1: Sync Material Types
async function syncMaterialTypes(limit = null) {
  return syncCollection({
    name: 'Material Types',
    collectionId: WEBFLOW_COLLECTIONS.materialType,
    mappingKey: 'materialType',
    sanityQuery: `
      *[_type == "materialType"] | order(sortOrder asc, name.en asc) {
        _id,
        name,
        description,
        sortOrder,
        slug
      }
    `,
    fieldMapper: mapMaterialTypeFields,
    limit
  })
}

// PHASE 2: Sync Finishes
async function syncFinishes(limit = null) {
  return syncCollection({
    name: 'Finishes',
    collectionId: WEBFLOW_COLLECTIONS.finish,
    mappingKey: 'finish',
    sanityQuery: `
      *[_type == "finish"] | order(name.en asc) {
        _id,
        name,
        description,
        slug
      }
    `,
    fieldMapper: mapMediumFinishFields,
    limit
  })
}

// PHASE 3: Sync Materials (with Material Type references)
async function syncMaterials(limit = null) {
  return syncCollection({
    name: 'Materials',
    collectionId: WEBFLOW_COLLECTIONS.material,
    mappingKey: 'material',
    sanityQuery: `
      *[_type == "material"] | order(name.en asc) {
        _id,
        name,
        description,
        materialType->{_id, name},
        slug
      }
    `,
    fieldMapper: (item, locale = 'en') => {
      const isGerman = locale === 'de-DE' || locale === 'de'
      return {
        ...mapMediumFinishFields(item, locale),
        'material-type': item.materialType?._id ? idMappings.materialType.get(item.materialType._id) || null : null,
        description: isGerman ? (item.description?.de || '') : (item.description?.en || '')
      }
    },
    limit
  })
}

// PHASE 4: Sync other collections
async function syncMediums(limit = null) {
  return syncCollection({
    name: 'Mediums',
    collectionId: WEBFLOW_COLLECTIONS.medium,
    mappingKey: 'medium',
    sanityQuery: `
      *[_type == "medium"] | order(name.en asc) {
        _id,
        name,
        description,
        slug
      }
    `,
    fieldMapper: mapMediumFinishFields,
    limit
  })
}

async function syncCategories(limit = null) {
  return syncCollection({
    name: 'Categories',
    collectionId: WEBFLOW_COLLECTIONS.category,
    mappingKey: 'category',
    sanityQuery: `
      *[_type == "category"] | order(title.en asc) {
        _id,
        title,
        description,
        slug
      }
    `,
    fieldMapper: mapCategoryFields,
    limit
  })
}

async function syncLocations(limit = null) {
  return syncCollection({
    name: 'Locations',
    collectionId: WEBFLOW_COLLECTIONS.location,
    mappingKey: 'location',
    sanityQuery: `
      *[_type == "location"] | order(name.en asc) {
        _id,
        name,
        type,
        address,
        city->{name},
        country->{name},
        website,
        times,
        phone,
        email,
        description,
        slug
      }
    `,
    fieldMapper: mapLocationFields,
    limit
  })
}

async function syncCreators(limit = null) {
  return syncCollection({
    name: 'Creators',
    collectionId: WEBFLOW_COLLECTIONS.creator,
    mappingKey: 'creator',
    sanityQuery: `
      *[_type == "creator"] | order(name asc) {
        _id,
        name,
        cover{
          asset->{
            _id,
            url,
            originalFilename
          },
          alt
        },
        image{
          asset->{
            _id,
            url,
            originalFilename
          },
          alt
        },
        biography,
        portrait,
        nationality,
        specialties,
        galleryImages,
        slug,
        website,
        email,
        birthYear,
        category
      }
    `,
    fieldMapper: mapCreatorFields,
    limit
  })
}

// PHASE 8: Sync Artworks
async function syncArtworks(limit = null) {
  // Custom artwork mapper with image handling
  const artworkCustomSync = async (item) => {
    // Simple URL-based image handling - let Webflow handle uploads
    const artworkImages = item.images?.map(image => {
      if (!image.asset?.url) return null
      
      // Create enhanced alt text
      const altText = image.alt?.en || image.alt?.de || ''
      const artworkName = item.name || item.workTitle?.en || item.workTitle?.de
      const creatorName = item.creator?.name
      
      let enhancedAltText = altText
      if (!enhancedAltText && artworkName) {
        const parts = []
        if (creatorName) parts.push(creatorName)
        if (artworkName) parts.push(artworkName)
        enhancedAltText = parts.join(' - ')
      }
      
      return {
        url: image.asset.url,
        alt: enhancedAltText || artworkName || 'Artwork image'
      }
    }).filter(Boolean) || []
    
    console.log(`  🖼️  Prepared ${artworkImages.length} images for upload via URLs`)
    
    // Validate and map references with error logging
    const creatorId = item.creator?._id ? idMappings.creator.get(item.creator._id) || null : null
    if (item.creator?._id && !creatorId) {
      console.warn(`  ⚠️  Creator reference not found for artwork '${item.name}': ${item.creator._id}`)
    }
    
    const categoryId = item.category?._id ? idMappings.category.get(item.category._id) || null : null
    if (item.category?._id && !categoryId) {
      console.warn(`  ⚠️  Category reference not found for artwork '${item.name}': ${item.category._id}`)
    }
    
    const materialIds = item.materials?.map(mat => {
      const mappedId = idMappings.material.get(mat._id)
      if (!mappedId) {
        console.warn(`  ⚠️  Material reference not found for artwork '${item.name}': ${mat._id}`)
      }
      return mappedId
    }).filter(Boolean) || []
    
    const mediumIds = item.medium?.map(med => {
      const mappedId = idMappings.medium.get(med._id)
      if (!mappedId) {
        console.warn(`  ⚠️  Medium reference not found for artwork '${item.name}': ${med._id}`)
      }
      return mappedId
    }).filter(Boolean) || []
    
    const finishIds = item.finishes?.map(fin => {
      const mappedId = idMappings.finish.get(fin._id)
      if (!mappedId) {
        console.warn(`  ⚠️  Finish reference not found for artwork '${item.name}': ${fin._id}`)
      }
      return mappedId
    }).filter(Boolean) || []
    
    // NEW: map single main image if present
    const mainImage = item.mainImage?.asset?.url ? {
      url: item.mainImage.asset.url,
      alt: (item.mainImage?.alt?.en || item.mainImage?.alt?.de || item.name || item.workTitle?.en || item.workTitle?.de || 'Main image')
    } : undefined

    // Note: customImageSync doesn't support locale parameter yet, always returns English
    // German locale will be updated separately after creation
    return {
      fieldData: {
        name: item.name || 'Untitled',
        slug: item.slug?.current || generateSlug(item.name || item.workTitle?.en),
        'work-title': item.workTitle?.en || item.workTitle?.de || '',
        description: item.description?.en || '',
        creator: creatorId,
        category: categoryId ? [categoryId] : [],
        materials: materialIds,
        medium: mediumIds,
        finishes: finishIds,
        'size-dimensions': item.size || '',
        year: item.year || '',
        price: item.price || '',
        ...(mainImage ? { 'main-image': mainImage } : {}),
        'artwork-images': artworkImages
      },
      _sanityItem: item // Store for German locale update
    }
  }

  return syncCollection({
    name: 'Artworks',
    collectionId: WEBFLOW_COLLECTIONS.artwork,
    mappingKey: 'artwork',
    sanityQuery: `
      *[_type == "artwork"] | order(name asc) {
        _id,
        name,
        workTitle,
        description,
        creator->{_id},
        category->{_id},
        materials[]->{_id},
        medium[]->{_id},
        finishes[]->{_id},
        size,
        year,
        price,
        slug,
        mainImage{
          asset->{
            _id,
            url,
            originalFilename
          },
          alt
        },
        images[]{ 
          asset->{
            _id,
            url,
            originalFilename
          },
          alt
        }
      }
    `,
    fieldMapper: null, // Not used since we use customImageSync
    customImageSync: artworkCustomSync,
    limit
  })
}

// Sync artworks only for a specific subset of creators (by Sanity IDs)
async function syncArtworksForCreators(creatorIds = []) {
  if (!Array.isArray(creatorIds) || creatorIds.length === 0) {
    console.log('⚪ No creator IDs provided for filtered artwork sync. Skipping.')
    return 0
  }

  // Local custom image sync (same as in syncArtworks)
  const artworkCustomSync = async (item) => {
    const artworkImages = item.images?.map(image => {
      if (!image.asset?.url) return null
      const altText = image.alt?.en || image.alt?.de || ''
      const artworkName = item.name || item.workTitle?.en || item.workTitle?.de
      const creatorName = item.creator?.name
      let enhancedAltText = altText
      if (!enhancedAltText && artworkName) {
        const parts = []
        if (creatorName) parts.push(creatorName)
        if (artworkName) parts.push(artworkName)
        enhancedAltText = parts.join(' - ')
      }
      return {
        url: image.asset.url,
        alt: enhancedAltText || artworkName || 'Artwork image'
      }
    }).filter(Boolean) || []

    console.log(`  🖼️  Prepared ${artworkImages.length} images for upload via URLs`)

    const creatorId = item.creator?._id ? idMappings.creator.get(item.creator._id) || null : null
    if (item.creator?._id && !creatorId) {
      console.warn(`  ⚠️  Creator reference not found for artwork '${item.name}': ${item.creator._id}`)
    }

    const categoryId = item.category?._id ? idMappings.category.get(item.category._id) || null : null
    if (item.category?._id && !categoryId) {
      console.warn(`  ⚠️  Category reference not found for artwork '${item.name}': ${item.category._id}`)
    }

    const materialIds = item.materials?.map(mat => idMappings.material.get(mat._id)).filter(Boolean) || []
    const mediumIds = item.medium?.map(med => idMappings.medium.get(med._id)).filter(Boolean) || []
    const finishIds = item.finishes?.map(fin => idMappings.finish.get(fin._id)).filter(Boolean) || []

    return {
      fieldData: {
        name: item.name || 'Untitled',
        slug: item.slug?.current || generateSlug(item.name || item.workTitle?.en),
        'work-title': item.workTitle?.en || item.workTitle?.de || '',
        'work-title-english': item.workTitle?.en || '',
        'work-title-german': item.workTitle?.de || '',
        'description-english': item.description?.en || '',
        'description-german': item.description?.de || '',
        creator: creatorId,
        category: categoryId ? [categoryId] : [],
        materials: materialIds,
        medium: mediumIds,
        finishes: finishIds,
        'size-dimensions': item.size || '',
        year: item.year || '',
        price: item.price || '',
        'artwork-images': artworkImages
      }
    }
  }

  // Inline IDs into query (simple and effective for a one-off filtered sync)
  const idsList = creatorIds.map(id => `"${id}"`).join(',')
  const filteredQuery = `
      *[_type == "artwork" && defined(creator._ref) && creator._ref in [${idsList}]] | order(name asc) {
        _id,
        name,
        workTitle,
        description,
        creator->{_id,name},
        category->{_id},
        materials[]->{_id},
        medium[]->{_id},
        finishes[]->{_id},
        size,
        year,
        price,
        slug,
        images[]{ 
          asset->{
            _id,
            url,
            originalFilename
          },
          alt
        }
      }
    `

  return syncCollection({
    name: 'Artworks (filtered by creators)',
    collectionId: WEBFLOW_COLLECTIONS.artwork,
    mappingKey: 'artwork',
    sanityQuery: filteredQuery,
    fieldMapper: null,
    customImageSync: artworkCustomSync,
    limit: null
  })
}

// PHASE 4: Populate Creator Works (Reverse Linkage)
async function populateCreatorWorks() {
  console.log('\n🔗 PHASE 4: Populating Creator Works (Reverse Linkage)')
  
  try {
    // Get ALL creators from Webflow with pagination
    let allCreators = []
    let creatorsOffset = 0
    let hasMoreCreators = true
    
    while (hasMoreCreators) {
      const creatorsResponse = await webflowRequest(`/collections/${WEBFLOW_COLLECTIONS.creator}/items?limit=100&offset=${creatorsOffset}`)
      allCreators = allCreators.concat(creatorsResponse.items)
      
      hasMoreCreators = creatorsResponse.items.length === 100
      creatorsOffset += 100
    }
    
    console.log(`📋 Found ${allCreators.length} creators to process`)
    
    // Get ALL artworks from Webflow with pagination  
    let allArtworks = []
    let artworksOffset = 0
    let hasMoreArtworks = true
    
    while (hasMoreArtworks) {
      const artworksResponse = await webflowRequest(`/collections/${WEBFLOW_COLLECTIONS.artwork}/items?limit=100&offset=${artworksOffset}`)
      allArtworks = allArtworks.concat(artworksResponse.items)
      
      hasMoreArtworks = artworksResponse.items.length === 100
      artworksOffset += 100
    }
    
    console.log(`🖼️  Found ${allArtworks.length} artworks to process`)
    
    // Process each creator
    for (let i = 0; i < allCreators.length; i++) {
      const creator = allCreators[i]
      const creatorName = creator.fieldData.name
      
      // Find all artworks that belong to this creator
      const creatorArtworks = allArtworks.filter(artwork => 
        artwork.fieldData.creator === creator.id
      )
      
      if (creatorArtworks.length > 0) {
        console.log(`  🎨 ${creatorName}: ${creatorArtworks.length} artworks`)
        
        // Update creator's works field with artwork IDs
        const artworkIds = creatorArtworks.map(artwork => artwork.id)
        
        await webflowRequest(`/collections/${WEBFLOW_COLLECTIONS.creator}/items/${creator.id}`, {
          method: 'PATCH',
          body: JSON.stringify({
            fieldData: {
              works: artworkIds
            }
          })
        })
      } else {
        console.log(`  ⚪ ${creatorName}: 0 artworks`)
      }
    }
    
    console.log(`✅ Creator works populated successfully`)
    
  } catch (error) {
    console.error(`❌ Error populating creator works:`, error)
    throw error
  }
}

// Main sync function
async function performCompleteSync(progressCallback = null, options = {}) {
  const { limitPerCollection = null, only = ARG_ONLY } = options || {}
  const startTime = Date.now()
  let totalSynced = 0
  
  const updateProgress = (step, message, currentCount = 0, totalCount = 0) => {
    if (progressCallback) {
      progressCallback({
        phase: step,
        message,
        currentCount,
        totalCount,
        totalSynced
      })
    }
  }
  
  try {
    console.log('🚀 Starting Complete Sanity → Webflow Sync')
    console.log('='.repeat(60))
    
    // Resolve Webflow collections for this site
    if (!WEBFLOW_COLLECTIONS) {
      WEBFLOW_COLLECTIONS = await resolveWebflowCollections()
      if (!WEBFLOW_COLLECTIONS) {
        throw new Error('Failed to resolve Webflow collections for the site')
      }
      console.log('🗂️  Resolved Webflow collections:', WEBFLOW_COLLECTIONS)
    }

    // Resolve Webflow locales
    await resolveWebflowLocales()
    console.log('🌍 Resolved Webflow locales:', WEBFLOW_LOCALES)

    // Load asset mappings for incremental image sync
    await loadAssetMappings()
    
    // Load ID mappings for persistent ID matching
    await loadIdMappings()
    loadPersistentMappings()
    
    // PHASE 0: Load existing ID mappings (smart sync - no clearing)
    console.log('\n🔗 PHASE 0: Loading existing ID mappings for smart sync')
    
    await loadIdMappings()
    loadPersistentMappings()
    
    // Rebuild mappings from existing Webflow data if needed
    await rebuildIdMappings()
    
    console.log('✅ Smart sync enabled - no duplicates will be created')
    
    // Phase 1: Foundation data (no dependencies)
    updateProgress('Phase 1', 'Starting foundation data sync...', 0, 4)
    console.log('\n📋 PHASE 1: Foundation Data')
    
    const phase1All = [
      { name: 'Material Types', key: 'materialType', func: syncMaterialTypes },
      { name: 'Finishes', key: 'finish', func: syncFinishes },
      { name: 'Categories', key: 'category', func: syncCategories },
      { name: 'Locations', key: 'location', func: syncLocations }
    ]
    const syncFunctions = (only ? phase1All.filter(p => p.key === only || normalize(p.name) === normalize(only)) : phase1All)
    
    for (let i = 0; i < syncFunctions.length; i++) {
      const { name, func } = syncFunctions[i]
      try {
        updateProgress('Phase 1', `Syncing ${name}...`, i + 1, 4)
        totalSynced += await func(limitPerCollection)
      } catch (error) {
        console.error(`❌ Failed to sync ${name}: ${error.message}`)
        updateProgress('Phase 1', `Failed to sync ${name}: ${error.message}`, i + 1, 4)
        // Continue with other collections instead of failing completely
      }
    }
    
    // Phase 2: Reference data (with dependencies)
    updateProgress('Phase 2', 'Starting reference data sync...', 0, 3)
    console.log('\n🔗 PHASE 2: Reference Data')
    
    const phase2All = [
      { name: 'Materials', key: 'material', func: syncMaterials },
      { name: 'Mediums', key: 'medium', func: syncMediums },
      { name: 'Creators', key: 'creator', func: syncCreators }
    ]
    const syncFunctions2 = (only ? phase2All.filter(p => p.key === only || normalize(p.name) === normalize(only)) : phase2All)
    
    for (let i = 0; i < syncFunctions2.length; i++) {
      const { name, func } = syncFunctions2[i]
      try {
        updateProgress('Phase 2', `Syncing ${name}...`, i + 1, 3)
        totalSynced += await func(limitPerCollection)
      } catch (error) {
        console.error(`❌ Failed to sync ${name}: ${error.message}`)
        updateProgress('Phase 2', `Failed to sync ${name}: ${error.message}`, i + 1, 3)
        // Continue with other collections instead of failing completely
      }
    }
    
    // Phase 3: Complex data (with multiple dependencies)
    updateProgress('Phase 3', 'Starting artwork sync...', 0, 1)
    console.log('\n🎨 PHASE 3: Complex Data')
    
    if (!only || only === 'artwork' || normalize(only) === 'artworks') {
      try {
        updateProgress('Phase 3', 'Syncing Artworks with Images...', 1, 1)
        totalSynced += await syncArtworks(limitPerCollection)
      } catch (error) {
        console.error(`❌ Failed to sync Artworks:`, error)
        updateProgress('Phase 3', `Failed to sync Artworks: ${error.message}`, 1, 1)
      }
    }
    
    // PHASE 4: Populate Creator Works (Reverse Linkage)
    if (!only || only === 'creator' || normalize(only) === 'creators') {
      try {
        updateProgress('Phase 4', 'Linking artworks to creators...', 1, 1)
        await populateCreatorWorks()
      } catch (error) {
        console.error(`❌ Failed to populate creator works:`, error)
        updateProgress('Phase 4', `Failed to populate creator works: ${error.message}`, 1, 1)
      }
    }
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(1)
    console.log(`\n✅ Complete sync finished in ${duration}s`)
    console.log(`📊 Total items synced: ${totalSynced}`)
    
    // Save asset mappings for future incremental syncs
    await saveAssetMappings()
    
    // Save ID mappings for future persistent ID matching
    await saveIdMappings()
    
    updateProgress('Complete', `Sync completed! ${totalSynced} items synced`, totalSynced, totalSynced)
    
    return {
      success: true,
      totalSynced,
      duration: `${duration}s`,
      timestamp: new Date().toISOString()
    }
    
  } catch (error) {
    console.error('❌ Sync failed:', error.message)
    if (progressCallback) {
      progressCallback({
        phase: 'Error',
        message: error.message,
        error: true
      })
    }
    throw error
  }
}

// Main API handler
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
    // Verify required environment variables
    if (!process.env.SANITY_API_TOKEN) {
      throw new Error('SANITY_API_TOKEN environment variable is required')
    }
    if (!process.env.WEBFLOW_API_TOKEN) {
      throw new Error('WEBFLOW_API_TOKEN environment variable is required')
    }
    
    // Check if client wants streaming progress and optional limit
    const { streaming, limit, limitPerCollection } = req.body || {}
    const limitValue = Number.isFinite(Number(limitPerCollection)) ? Number(limitPerCollection) : (Number.isFinite(Number(limit)) ? Number(limit) : (process.env.LIMIT_PER_COLLECTION ? Number(process.env.LIMIT_PER_COLLECTION) : null))
    
    if (streaming) {
      // Set up Server-Sent Events
      res.setHeader('Content-Type', 'text/event-stream')
      res.setHeader('Cache-Control', 'no-cache')
      res.setHeader('Connection', 'keep-alive')
      
      const sendProgress = (progress) => {
        res.write(`data: ${JSON.stringify({ type: 'progress', ...progress })}\n\n`)
      }
      
      try {
        console.log('🔔 Sync triggered via API (streaming)')
        const result = await performCompleteSync(sendProgress, { limitPerCollection: limitValue })
        res.write(`data: ${JSON.stringify({ type: 'complete', result })}\n\n`)
        res.end()
      } catch (error) {
        res.write(`data: ${JSON.stringify({ type: 'error', error: error.message })}\n\n`)
        res.end()
      }
    } else {
      // Regular sync without streaming
      console.log('🔔 Sync triggered via API')
      const result = await performCompleteSync(null, { limitPerCollection: limitValue })
      
      res.status(200).json({
        message: 'Sync completed successfully',
        ...result
      })
    }
    
  } catch (error) {
    console.error('API Error:', error.message)
    res.status(500).json({
      error: 'Sync failed',
      message: error.message,
      timestamp: new Date().toISOString()
    })
  }
} 

// Allow running directly from command line
if (require.main === module) {
  console.log('🚀 Running sync directly...')
  const cliLimit = Number(getArg('limit')) || null
  const cliCreatorsForArtworks = getArg('artworks-for-creators')

  const limitValue = cliLimit ?? (process.env.LIMIT_PER_COLLECTION ? Number(process.env.LIMIT_PER_COLLECTION) : null)

  const run = async () => {
    // Resolve collections and locales first
    WEBFLOW_COLLECTIONS = await resolveWebflowCollections()
    await resolveWebflowLocales()
    
    if (cliCreatorsForArtworks) {
      await loadIdMappings();
      loadPersistentMappings();
      const ids = cliCreatorsForArtworks.split(',').map(s => s.trim()).filter(Boolean)
      await syncArtworksForCreators(ids)
      await populateCreatorWorks()
      return
    }

    await performCompleteSync((progress) => {
      console.log(`[${progress.phase}] ${progress.message}`)
    }, { limitPerCollection: limitValue })
  }

  run().then(() => {
    console.log('✅ Sync completed!')
    process.exit(0)
  }).catch((error) => {
    console.error('❌ Sync failed:', error.message)
    process.exit(1)
  })
  // debugImageUpload().catch(console.error)
} 

// New isolated test function for debugging image uploads
async function debugImageUpload() {
  console.log('🧪 Starting isolated image upload debug...')

  // 1. Load mappings needed for the test
  await loadAssetMappings()
  
  // 2. Fetch one artwork with images from Sanity
  const testArtwork = await sanityClient.fetch(`
    *[_type == "artwork" && count(images) > 0][0] {
      _id,
      name,
      images[]{ 
        asset->{
          _id,
          url,
          originalFilename
        },
        alt
      }
    }
  `)

  if (!testArtwork) {
    console.error('❌ No artworks with images found to test.')
    return
  }

  console.log(`🖼️  Testing with artwork: "${testArtwork.name}" (${testArtwork.images.length} images)`)

  // 3. Run only the image sync logic
  try {
    const webflowAssetIds = await syncArtworkImages(testArtwork.images)
    console.log('✅ Debug image sync complete.')
    console.log('Uploaded Webflow Asset IDs:', webflowAssetIds)
    
    // 4. Save any new asset mappings
    await saveAssetMappings()

  } catch (error) {
    console.error('❌ Debug image sync failed:', error)
  }
}

// To run the debug function: comment out the performCompleteSync call and uncomment this
// if (require.main === module) {
//   debugImageUpload().catch(console.error)
// } 