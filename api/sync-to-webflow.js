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
  'en-US': null,  // Primary locale ID (use en-US key for consistency)
  'de-DE': null   // German locale ID
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
const FLAG_PUBLISH = true // ALWAYS publish items to Webflow after sync
const ARG_ONLY = getArg('only') // e.g. --only=creator|artwork|material
const ARG_ITEM = getArg('item') // e.g. --item=creator-id-123 (single item sync)
const FLAG_ENGLISH_ONLY = ARGS.includes('--english-only')

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
      // With Advanced Localization, we can use the correct locale IDs
      if (siteInfo.locales.primary?.cmsLocaleId) {
        WEBFLOW_LOCALES['en-US'] = siteInfo.locales.primary.cmsLocaleId
        console.log(`  🌍 Primary locale (en-US): ${WEBFLOW_LOCALES['en-US']}`)
      }
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
    medium: pick('type', 'types', 'media', 'medium'), // Sanity medium (Type) → Webflow Type
    category: pick('medium', 'mediums', 'category', 'categories'), // Sanity category (Medium) → Webflow Medium
    location: pick('locations', 'location'),
    creator: pick('creators', 'creator', 'profiles', 'profile'),
    artwork: pick('artworks', 'artwork', 'works', 'work'),
    article: pick('articles', 'article'),
    author: pick('authors', 'author'),
    photographer: pick('photographers', 'photographer')
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
  artwork: new Map(),
  article: new Map(),
  author: new Map(),
  photographer: new Map()
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
    { key: 'article', id: WEBFLOW_COLLECTIONS.article, sanityType: 'article' },
    { key: 'author', id: WEBFLOW_COLLECTIONS.author, sanityType: 'author' },
    { key: 'photographer', id: WEBFLOW_COLLECTIONS.photographer, sanityType: 'photographer' },
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

// Clean up size/dimensions field - remove leading commas, normalize spacing
function cleanSizeField(size) {
  if (!size || typeof size !== 'string') return ''
  
  return size
    .trim()
    .replace(/^,\s*/, '') // Remove leading comma
    .replace(/,\s*$/, '') // Remove trailing comma
    .replace(/\s*,\s*/g, ', ') // Normalize comma spacing
    .replace(/\s+/g, ' ') // Normalize whitespace
    .trim()
}
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

// Convert Sanity block content to Webflow's Rich Text HTML format
function convertSanityBlocksToWebflowRichText(blocks) {
  if (!blocks || !Array.isArray(blocks)) return null
  
  const htmlElements = blocks.map(block => {
    if (block._type === 'block' && block.children) {
      let paragraphContent = ''
      
      // Process each text span
      for (const child of block.children) {
        let text = child.text || ''
        
        // Apply formatting marks
        if (child.marks && child.marks.length > 0) {
          // Track which marks to apply
          const marks = {
            strong: false,      // Bold
            em: false,          // Italic
            underline: false,   // Underline
            strike: false,      // Strike-through
            link: null          // Link URL
          }
          
          // Parse all marks
          for (const mark of child.marks) {
            if (mark === 'strong') marks.strong = true
            else if (mark === 'em') marks.em = true
            else if (mark === 'underline') marks.underline = true
            else if (mark === 'strike-through') marks.strike = true
            else if (typeof mark === 'object' && mark._type === 'link') {
              marks.link = mark.href
            }
          }
          
          // Apply marks in proper nesting order (inside-out)
          if (marks.link) text = `<a href="${marks.link}">${text}</a>`
          if (marks.strong) text = `<strong>${text}</strong>`
          if (marks.em) text = `<em>${text}</em>`
          if (marks.underline) text = `<u>${text}</u>`
          if (marks.strike) text = `<s>${text}</s>`
        }
        
        paragraphContent += text
      }
      
      // Wrap in appropriate block element based on style
      const style = block.style || 'normal'
      switch (style) {
        case 'h1': return `<h1>${paragraphContent}</h1>`
        case 'h2': return `<h2>${paragraphContent}</h2>`
        case 'h3': return `<h3>${paragraphContent}</h3>`
        case 'h4': return `<h4>${paragraphContent}</h4>`
        case 'blockquote': return `<blockquote>${paragraphContent}</blockquote>`
        default: return `<p>${paragraphContent}</p>`
      }
    } 
    // Handle list items
    else if (block._type === 'block' && block.listItem) {
      let listContent = ''
      for (const child of block.children || []) {
        listContent += child.text || ''
      }
      
      if (block.listItem === 'bullet') {
        return `<ul><li>${listContent}</li></ul>`
      } else if (block.listItem === 'number') {
        return `<ol><li>${listContent}</li></ol>`
      }
    }
    return null
  }).filter(Boolean)
  
  if (htmlElements.length === 0) return null
  
  return htmlElements.join('')
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
    'last-name': sanityItem.lastName || '',
    'slug': sanityItem.slug?.current || generateSlug(sanityItem.name),
    'hero-image': (sanityItem.cover?.asset?.url ? {
      url: sanityItem.cover.asset.url,
      alt: (sanityItem.cover?.alt?.en || sanityItem.cover?.alt?.de || sanityItem.name || '')
    } : undefined),
    'profile-image': (sanityItem.image?.asset?.url ? {
      url: sanityItem.image.asset.url,
      alt: (sanityItem.image?.alt?.en || sanityItem.image?.alt?.de || sanityItem.name || '')
    } : undefined),
    'studio-image': (sanityItem.studioImage?.asset?.url ? {
      url: sanityItem.studioImage.asset.url,
      alt: (sanityItem.studioImage?.alt?.en || sanityItem.studioImage?.alt?.de || sanityItem.name || '')
    } : undefined),
    'portrait-image': (sanityItem.portraitImage?.asset?.url ? {
      url: sanityItem.portraitImage.asset.url,
      alt: (sanityItem.portraitImage?.alt?.en || sanityItem.portraitImage?.alt?.de || sanityItem.name || '')
    } : undefined),
    'website': sanityItem.website || '',
    'email': sanityItem.email || '',
    'birth-year': sanityItem.birthYear ? parseInt(sanityItem.birthYear, 10) : null,
    'category': sanityItem.category?._ref ? idMappings.category.get(sanityItem.category._ref) || null : null,
    'locations': (sanityItem.associatedLocations || [])
      .map(loc => loc._ref ? idMappings.location.get(loc._ref) : null)
      .filter(Boolean)
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

// Global rate limiter - add delay before EVERY API request
let lastRequestTime = 0
const MIN_REQUEST_INTERVAL = 1200 // Minimum 1.2 seconds between ANY requests

// Webflow API helper with rate limit handling
async function webflowRequest(endpoint, options = {}, retryCount = 0) {
  // Global rate limiting - ensure minimum time between requests
  const now = Date.now()
  const timeSinceLastRequest = now - lastRequestTime
  if (timeSinceLastRequest < MIN_REQUEST_INTERVAL) {
    await sleep(MIN_REQUEST_INTERVAL - timeSinceLastRequest)
  }
  lastRequestTime = Date.now()
  
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

// Update one item in a Webflow collection (using bulk PATCH with cmsLocaleId in payload)
async function updateWebflowItem(collectionId, itemId, fieldData, localeId = null) {
  try {
    await sleep(1000) // throttle to avoid rate limits
    
    // DEBUG: Log what we're sending
    if (fieldData.name === 'Tora Urup') {
      console.log(`\n🐛 DEBUG - Updating Tora Urup:`)
      console.log(`   Locale: ${localeId || 'PRIMARY'}`)
      console.log(`   Portrait: ${fieldData['portrait-english']?.substring(0, 60)}...`)
      console.log(`   Biography: ${fieldData['biography']?.substring(0, 60)}...`)
    }
    
    // Use bulk PATCH with cmsLocaleId in the payload (works with Advanced Localization)
    const itemPayload = {
      id: itemId,
      fieldData
    }
    
    // Include cmsLocaleId in payload if specified
    if (localeId) {
      itemPayload.cmsLocaleId = localeId
    }
    
    const result = await webflowRequest(`/collections/${collectionId}/items`, {
      method: 'PATCH',
      body: JSON.stringify({ items: [itemPayload] })
    })
    
    // Return the first item from the response
    return result?.items?.[0] || result
  } catch (error) {
    console.error(`Update failed for ${itemId} (locale: ${localeId || 'primary'}):`, error.message)
    throw error
  }
}

// Create/update secondary locale (German) for an item
async function updateItemGermanLocale(collectionId, itemId, sanityItem, fieldMapper) {
  if (!WEBFLOW_LOCALES['de-DE']) {
    return // Skip if German locale not available
  }
  
  try {
    // For artworks with customImageSync, sanityItem might be a wrapped object
    const actualItem = sanityItem._sanityItem || sanityItem
    
    let germanFields = {}
    
    if (fieldMapper) {
      const result = await fieldMapper(actualItem, 'de-DE')
      // If result has .fieldData (customImageSync), unwrap it
      germanFields = result?.fieldData || result || {}
    }
    
    // For artworks without standard fieldMapper, manually map German content
    if (Object.keys(germanFields).length === 0 && actualItem.workTitle) {
      germanFields['work-title'] = actualItem.workTitle?.de || ''
      germanFields['description'] = actualItem.description?.de || ''
    }
    
    // Use PATCH to update existing item's German locale (not POST which creates new item)
    await webflowRequest(`/collections/${collectionId}/items/${itemId}`, {
      method: 'PATCH',
      body: JSON.stringify({
        cmsLocaleId: WEBFLOW_LOCALES['de-DE'],
        fieldData: germanFields
      })
    })
    
    console.log(`    🇩🇪 Updated German locale`)
  } catch (error) {
    console.warn(`    ⚠️  Failed to create/update German locale: ${error.message}`)
  }
}

// Create items in Webflow using bulk endpoint with both locales
async function createWebflowItems(collectionId, items, progressCallback = null) {
  const results = []
  
  // Process items one at a time to properly create linked EN+DE variants
  for (let i = 0; i < items.length; i++) {
    const item = items[i]
    console.log(`Creating item ${i + 1}/${items.length}...`)
    
    try {
      // Step 1: Create linked item in BOTH locales using /bulk endpoint
      // This should create ONE item with both EN and DE locales (same ID)
      // Filter out undefined values from fieldData to prevent validation errors
      const cleanFieldData = Object.fromEntries(
        Object.entries(item.fieldData).filter(([_, v]) => v !== undefined)
      )
      
      if (i < 3) {
        console.log(`  🧪 Payload sample (${i + 1}):`) 
        console.log(`    raw keys: ${Object.keys(item.fieldData).join(', ')}`)
        console.log(`    clean keys: ${Object.keys(cleanFieldData).join(', ')}`)
        console.log('    clean fieldData:', JSON.stringify(cleanFieldData, null, 2))
      }

      const createResult = await webflowRequest(`/collections/${collectionId}/items/bulk`, {
        method: 'POST',
        body: JSON.stringify({
          cmsLocaleIds: [WEBFLOW_LOCALES['en-US'], WEBFLOW_LOCALES['de-DE']],
          isDraft: !FLAG_PUBLISH,
          fieldData: cleanFieldData
        })
      })
      
      const createdItem = createResult?.items?.[0]
      if (!createdItem) {
        throw new Error('No item returned from bulk create')
      }
      
      results.push(createdItem)
      console.log(`  ✅ Created EN+DE: ${createdItem.id}`)
      
      // Emit progress event for every item (progress bar)
      if (progressCallback) {
        progressCallback({
          progress: {
            phase: 'Creating items',
            message: `Created ${i + 1} of ${items.length}`,
            current: i + 1,
            total: items.length
          }
        })
      }
      
      // Emit toast notification for first, milestones, and last items
      if (progressCallback && (i === 0 || (i + 1) % 25 === 0 || i === items.length - 1)) {
        progressCallback({
          itemCreated: item.fieldData?.name || item.fieldData?.slug || createdItem.id
        })
      }
      
      // Step 2: Update DE locale with German-specific content if available
      // (The bulk create uses EN content by default, now we patch DE)
      if (item.germanFieldData) {
        try {
          console.log(`  🔍 German fields present: ${Object.keys(item.germanFieldData).length} fields`)
          console.log(`  🔍 Sample German data:`, JSON.stringify({
            'hero-headline': item.germanFieldData['hero-headline'],
            'intro': item.germanFieldData['intro']?.substring(0, 50),
            'section-1-text-2': item.germanFieldData['section-1-text-2']?.substring(0, 50)
          }, null, 2))
          await sleep(800)
          await webflowRequest(`/collections/${collectionId}/items/${createdItem.id}`, {
            method: 'PATCH',
            body: JSON.stringify({
              cmsLocaleId: WEBFLOW_LOCALES['de-DE'],
              fieldData: item.germanFieldData
            })
          })
          console.log(`  🇩🇪 Updated German content`)
        } catch (deError) {
          console.warn(`  ⚠️  Failed to update DE locale: ${deError.message}`)
        }
      } else {
        console.log(`  ⚠️  No germanFieldData - DE will have EN content`)
      }
      
      // Step 3: Publish if requested
      if (FLAG_PUBLISH) {
        try {
          await webflowRequest(`/collections/${collectionId}/items/publish`, {
            method: 'POST',
            body: JSON.stringify({
              items: [{
                id: createdItem.id,
                cmsLocaleIds: [WEBFLOW_LOCALES['en-US'], WEBFLOW_LOCALES['de-DE']]
              }]
            })
          })
          console.log(`  🚀 Published both locales`)
        } catch (publishError) {
          console.warn(`  ⚠️  Failed to publish: ${publishError.message}`)
        }
      }
      
    } catch (error) {
      console.error(`  ❌ Failed to create item:`, error.message)
      throw error
    }
  }
  
  return results
}

// Publish items in batches (publishes BOTH EN and DE locales)
async function publishWebflowItems(collectionId, itemIds, progressCallback = null) {
  const batchSize = 50
  for (let i = 0; i < itemIds.length; i += batchSize) {
    const batch = itemIds.slice(i, i + batchSize)
    let attempt = 0
    const maxAttempts = 3
    while (attempt < maxAttempts) {
      try {
        console.log(`  🚀 Publishing batch ${Math.floor(i/batchSize)+1}/${Math.ceil(itemIds.length/batchSize)} (${batch.length} items)...`)
        
        // Use new format with cmsLocaleIds to publish BOTH EN and DE locales
        await webflowRequest(`/collections/${collectionId}/items/publish`, {
          method: 'POST',
          body: JSON.stringify({ 
            items: batch.map(id => ({
              id,
              cmsLocaleIds: [WEBFLOW_LOCALES['en-US'], WEBFLOW_LOCALES['de-DE']]
            }))
          })
        })
        
        // Emit progress event
        if (progressCallback) {
          progressCallback({
            progress: {
              phase: 'Publishing items',
              message: `Published ${Math.min(i + batchSize, itemIds.length)} of ${itemIds.length}`,
              current: Math.min(i + batchSize, itemIds.length),
              total: itemIds.length
            }
          })
        }
        
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
// Fetches from ALL locales to detect orphans in secondary locales
async function getWebflowItems(collectionId) {
  try {
    let allItemsById = new Map() // Use Map to dedupe by ID
    let offset = 0
    const limit = 100
    
    // Fetch from primary locale (EN)
    while (true) {
      await sleep(500) // Add delay between pagination requests
      const result = await webflowRequest(`/collections/${collectionId}/items?limit=${limit}&offset=${offset}`)
      const items = result.items || []
      
      items.forEach(item => allItemsById.set(item.id, item))
      
      // If we got fewer items than the limit, we've reached the end
      if (items.length < limit) {
        break
      }
      
      offset += limit
    }
    
    // Also fetch from German locale to catch orphans that only exist there
    if (WEBFLOW_LOCALES['de-DE']) {
      offset = 0
      while (true) {
        await sleep(500)
        const result = await webflowRequest(`/collections/${collectionId}/items?limit=${limit}&offset=${offset}&locale=${WEBFLOW_LOCALES['de-DE']}`)
        const items = result.items || []
        
        items.forEach(item => {
          // Only add if not already in map (primary locale takes precedence)
          if (!allItemsById.has(item.id)) {
            allItemsById.set(item.id, item)
          }
        })
        
        if (items.length < limit) {
          break
        }
        
        offset += limit
      }
    }
    
    const allItems = Array.from(allItemsById.values())
    
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
  // Create a version that includes image asset metadata for reliable change detection
  const objWithImageMetadata = addImageMetadataToHash(obj)
  const json = JSON.stringify(objWithImageMetadata, Object.keys(objWithImageMetadata).sort())
  return generateMD5Hash(Buffer.from(json))
}

// Helper to include image asset IDs/URLs in hash for reliable change detection
// This ensures image changes (new URLs) are detected even if other fields are unchanged
function addImageMetadataToHash(obj) {
  if (!obj || typeof obj !== 'object') return obj
  if (Array.isArray(obj)) return obj.map(addImageMetadataToHash)
  
  const result = {}
  for (const [key, value] of Object.entries(obj)) {
    // For image objects {url, alt}, include the URL in hash (URL changes = image changed)
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      if (value.url && typeof value.url === 'string') {
        // Image object - include URL in hash to detect image changes
        result[key] = {
          ...value,
          _hashUrl: value.url // Include URL in hash to detect image changes
        }
      } else {
        // Nested object - recurse
        result[key] = addImageMetadataToHash(value)
      }
    } else if (Array.isArray(value)) {
      // Array - check if it contains image objects
      result[key] = value.map(item => {
        if (item && typeof item === 'object' && item.url) {
          return { ...item, _hashUrl: item.url }
        }
        return addImageMetadataToHash(item)
      })
    } else {
      result[key] = value
    }
  }
  return result
}

// Extract all image URLs from fieldData (for comparison)
function extractImageUrls(obj, urls = []) {
  if (!obj || typeof obj !== 'object') return urls
  if (Array.isArray(obj)) {
    obj.forEach(item => {
      if (item && typeof item === 'object' && item.url) {
        urls.push(item.url)
      } else {
        extractImageUrls(item, urls)
      }
    })
  } else {
    Object.values(obj).forEach(value => {
      if (value && typeof value === 'object') {
        if (value.url && typeof value.url === 'string') {
          urls.push(value.url)
        } else {
          extractImageUrls(value, urls)
        }
      }
    })
  }
  return urls
}

// Compare image URLs from Webflow vs Sanity to detect changes
// This is the reliable way: compare actual URLs, not timestamps
async function checkIfImagesChanged(webflowItemId, newImageUrls, collectionId) {
  if (!webflowItemId || newImageUrls.length === 0) return false
  
  try {
    // Fetch current Webflow item to get existing image URLs
    const currentItem = await webflowRequest(`/collections/${collectionId}/items/${webflowItemId}`)
    if (!currentItem || !currentItem.fieldData) return false
    
    const currentImageUrls = extractImageUrls(currentItem.fieldData)
    
    // Compare URLs - if different, images changed
    const currentSorted = currentImageUrls.sort().join('|')
    const newSorted = newImageUrls.sort().join('|')
    
    if (currentSorted !== newSorted) {
      console.log(`  🖼️  Image URLs differ: ${currentImageUrls.length} current vs ${newImageUrls.length} new`)
      return true
    }
    
    return false
  } catch (error) {
    // If we can't fetch, assume no change (don't force update on error)
    console.warn(`  ⚠️  Could not check image changes: ${error.message}`)
    return false
  }
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
async function syncCollection(options, progressCallback = null) {
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
      // New item - prepare for creation with both EN and DE content
      const webflowItem = { 
        fieldData: mappedFieldsForId,
        germanFieldData: null // Will be populated below if German content exists
      }
      
      // Prepare German-specific field data if item has DE locale content
      if (customImageSync && typeof customImageSync === 'function') {
        try {
          const germanResult = await customImageSync(item, 'de-DE')
          const germanFields = germanResult.fieldData || germanResult
          if (germanFields && Object.keys(germanFields).length > 0) {
            webflowItem.germanFieldData = germanFields
            console.log(`  🔍 Prepared ${Object.keys(germanFields).length} German fields for new item (customImageSync)`)
          }
        } catch (e) {
          console.warn(`  ⚠️  German mapping failed (customImageSync): ${e.message}`)
        }
      } else if (fieldMapper && typeof fieldMapper === 'function') {
        // fieldMapper accepts (item, locale) - get German fields
        try {
          const germanFields = fieldMapper(item, 'de-DE')
          if (germanFields && Object.keys(germanFields).length > 0) {
            webflowItem.germanFieldData = germanFields
            console.log(`  🔍 Prepared ${Object.keys(germanFields).length} German fields for new item`)
          }
        } catch (e) {
          console.warn(`  ⚠️  German mapping failed: ${e.message}`)
        }
      }
      
      newItems.push({ item, webflowItem })
    } else {
      // Existing item - check if update is needed via delta hash
      const mapped = { ...mappedFieldsForId }
      delete mapped.slug // Don't change slug on update to avoid conflicts
      const webflowItem = { fieldData: mapped }
      
      // Also prepare German fields for updates
      if (customImageSync && typeof customImageSync === 'function') {
        try {
          const germanResult = await customImageSync(item, 'de-DE')
          const germanFields = germanResult.fieldData || germanResult
          if (germanFields && Object.keys(germanFields).length > 0) {
            webflowItem.germanFieldData = germanFields
          }
        } catch (e) {
          console.warn(`  ⚠️  German mapping failed for update (customImageSync): ${e.message}`)
        }
      } else if (fieldMapper && typeof fieldMapper === 'function') {
        try {
          const germanFields = fieldMapper(item, 'de-DE')
          if (germanFields && Object.keys(germanFields).length > 0) {
            webflowItem.germanFieldData = germanFields
          }
        } catch (e) {
          console.warn(`  ⚠️  German mapping failed for update: ${e.message}`)
        }
      }
      
      const hash = hashObjectStable(webflowItem.fieldData)
      const key = `${mappingKey}:${item._id}`
      const prev = persistentHashes.get(key)
      
      // Check both global and env var for force update
      const forceUpdate = global.FORCE_UPDATE || process.env.FORCE_UPDATE === 'true'
      
      // Extract image URLs from new fieldData for comparison
      const newImageUrls = extractImageUrls(webflowItem.fieldData)
      
      // Compare with Webflow's current image URLs (reliable detection)
      const imagesChanged = newImageUrls.length > 0 
        ? await checkIfImagesChanged(existingId, newImageUrls, collectionId)
        : false
      
      if (prev !== hash || forceUpdate || imagesChanged) {
        if (imagesChanged && prev === hash) {
          console.log(`  🖼️  Image change detected for ${item.name || item.creatorName || item._id}`)
        }
        updateItems.push({ item, webflowId: existingId, webflowItem, hash, key })
      } else {
        existingCount++
      }
    }
  }
  
  console.log(`  📊 ${newItems.length} new, ${updateItems.length} to update, ${existingCount} existing`)
  
  // Find and delete orphaned items (in Webflow but not in Sanity)
  // SKIP ORPHAN DELETION IN SINGLE-ITEM MODE to avoid deleting everything else
  const isSingleItemSync = !!global.SINGLE_ITEM_FILTER
  if (!isSingleItemSync) {
    // Build a set of "claimed" Webflow IDs (mapped or adopted by Sanity items)
    const claimedWebflowIds = new Set()
    
    // Add all mapped Webflow IDs
    for (const [sanityId, webflowId] of idMappings[mappingKey]) {
      claimedWebflowIds.add(webflowId)
    }
    
    // Add Webflow IDs from items we're about to create/update
    newItems.forEach(ni => {
      // New items will get IDs after creation, so can't add them here
    })
    updateItems.forEach(ui => {
      if (ui.webflowId) claimedWebflowIds.add(ui.webflowId)
    })
    
    // Also track which slugs are claimed by Sanity
    const sanitySlugToWebflowId = new Map()
    for (const item of sanityData) {
      const slug = item.slug?.current
      if (!slug) continue
      
      // Find the Webflow ID for this Sanity item
      const webflowId = idMappings[mappingKey].get(item._id)
      if (webflowId) {
        sanitySlugToWebflowId.set(slug, webflowId)
      } else {
        // Check if it was adopted by slug
        const wfItem = webflowBySlug.get(slug)
        if (wfItem) {
          sanitySlugToWebflowId.set(slug, wfItem.id)
          claimedWebflowIds.add(wfItem.id)
        }
      }
    }
    
    // Any Webflow item NOT claimed is an orphan
    // This handles: items with no Sanity match, AND duplicate items with the same slug
    const orphanedItems = existingWebflowItems.filter(wfItem => {
      return !claimedWebflowIds.has(wfItem.id)
    })
    
    if (orphanedItems.length > 0) {
      console.log(`  🗑️  Deleting ${orphanedItems.length} orphaned items from Webflow...`)
      orphanedItems.forEach(item => {
        console.log(`    - ${item.fieldData?.name || 'Unnamed'} (${item.fieldData?.slug || 'no-slug'}) - ${item.id}`)
      })
      const orphanedIds = orphanedItems.map(item => item.id)
      await deleteWebflowItems(collectionId, orphanedIds)
      // Clean up mappings
      orphanedItems.forEach(wfItem => {
        for (const [sanityId, webflowId] of idMappings[mappingKey]) {
          if (webflowId === wfItem.id) {
            idMappings[mappingKey].delete(sanityId)
            persistentHashes.delete(`${mappingKey}:${sanityId}`)
          }
        }
      })
    }
  } else {
    console.log(`  ⚡ Single-item mode: skipping orphan deletion`)
  }
  
  // Create new items in Webflow (primary locale)
  let results = []
  if (newItems.length > 0) {
    results = await createWebflowItems(collectionId, newItems.map(ni => ni.webflowItem), progressCallback)
    
    // Store new mappings and explicitly update both locales
    for (let index = 0; index < results.length; index++) {
      const webflowItem = results[index]
      const sanityItem = newItems[index].item
      idMappings[mappingKey].set(sanityItem._id, webflowItem.id)
      // Store hash for newly created items
      const hash = hashObjectStable(webflowItem.fieldData)
      persistentHashes.set(`${mappingKey}:${sanityItem._id}`, hash)
      
      // Locales already created by createWebflowItems() - no need to update again here
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
      // DEBUG: Check what we're sending for Tora Urup
      if (u.item.name === 'Tora Urup') {
        console.log(`\n🐛 DEBUG Tora Urup PRIMARY locale update:`)
        console.log(`   fieldMapper(item, 'en') returns:`)
        const testEn = fieldMapper(u.item, 'en')
        console.log(`   Portrait: ${testEn['portrait-english']?.substring(0,60)}...`)
        console.log(`   Biography: ${testEn['biography']?.substring(0,60)}...`)
        console.log(`\n   u.webflowItem.fieldData contains:`)
        console.log(`   Portrait: ${u.webflowItem.fieldData['portrait-english']?.substring(0,60)}...`)
        console.log(`   Biography: ${u.webflowItem.fieldData['biography']?.substring(0,60)}...`)
      }
      
      // Update primary locale with English content
      await updateWebflowItem(collectionId, u.webflowId, u.webflowItem.fieldData, FLAG_ENGLISH_ONLY ? null : WEBFLOW_LOCALES['en-US'])
      persistentHashes.set(u.key, u.hash)
      updatedItemIds.push(u.webflowId)
      
      // Create/update German locale (skip if english-only)
      if (!FLAG_ENGLISH_ONLY && (fieldMapper || customImageSync)) {
        // DEBUG: Check German locale data
        if (u.item.name === 'Tora Urup') {
          console.log(`\n🐛 DEBUG Tora Urup GERMAN locale update:`)
          console.log(`   fieldMapper(item, 'de-DE') returns:`)
          const testDe = fieldMapper(u.item, 'de-DE')
          console.log(`   Portrait: ${testDe['portrait-english']?.substring(0,60)}...`)
          console.log(`   Biography: ${testDe['biography']?.substring(0,60)}...`)
        }
        
        await sleep(800) // Extra delay between primary and German locale (increased from 300ms)
        await updateItemGermanLocale(collectionId, u.webflowId, u.item, fieldMapper || customImageSync)
      }
      
      updatedCount++
      if ((i + 1) % 25 === 0 || i === updateItems.length - 1) {
        console.log(`    ↳ Updated ${i + 1}/${updateItems.length}`)
      }
    } catch (e) {
      if (String(e.message).includes('429')) {
        await sleep(3000) // Longer delay on rate limit (increased from 1500ms)
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
    await publishWebflowItems(collectionId, updatedItemIds, progressCallback)
  }

  const unchangedCount = Math.max(existingCount - updateItems.length, 0)
  console.log(`✅ ${name}: ${results.length} created, ${updatedCount} updated, ${unchangedCount} unchanged`)
  return results.length + updatedCount
}

// PHASE 1: Sync Material Types
async function syncMaterialTypes(limit = null, progressCallback = null) {
  const filter = global.SINGLE_ITEM_FILTER || ''
  return syncCollection({
    name: 'Material Types',
    collectionId: WEBFLOW_COLLECTIONS.materialType,
    mappingKey: 'materialType',
    sanityQuery: `
      *[_type == "materialType" ${filter}] | order(sortOrder asc, name.en asc) {
        _id,
        name,
        description,
        sortOrder,
        slug
      }
    `,
    fieldMapper: mapMaterialTypeFields,
    limit
  }, progressCallback)
}

// PHASE 2: Sync Finishes
async function syncFinishes(limit = null, progressCallback = null) {
  const filter = global.SINGLE_ITEM_FILTER || ''
  return syncCollection({
    name: 'Finishes',
    collectionId: WEBFLOW_COLLECTIONS.finish,
    mappingKey: 'finish',
    sanityQuery: `
      *[_type == "finish" ${filter}] | order(name.en asc) {
        _id,
        name,
        description,
        slug
      }
    `,
    fieldMapper: mapMediumFinishFields,
    limit
  }, progressCallback)
}

// PHASE 3: Sync Materials (with Material Type references)
async function syncMaterials(limit = null, progressCallback = null) {
  const filter = global.SINGLE_ITEM_FILTER || ''
  return syncCollection({
    name: 'Materials',
    collectionId: WEBFLOW_COLLECTIONS.material,
    mappingKey: 'material',
    sanityQuery: `
      *[_type == "material" ${filter}] | order(name.en asc) {
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
  }, progressCallback)
}

// PHASE 4: Sync other collections
async function syncMediums(limit = null, progressCallback = null) {
  const filter = global.SINGLE_ITEM_FILTER || ''
  return syncCollection({
    name: 'Types',
    collectionId: WEBFLOW_COLLECTIONS.medium,
    mappingKey: 'medium',
    sanityQuery: `
      *[_type == "medium" ${filter}] | order(name.en asc) {
        _id,
        name,
        description,
        slug
      }
    `,
    fieldMapper: mapMediumFinishFields,
    limit
  }, progressCallback)
}

async function syncCategories(limit = null, progressCallback = null) {
  const filter = global.SINGLE_ITEM_FILTER || ''
  return syncCollection({
    name: 'Mediums',
    collectionId: WEBFLOW_COLLECTIONS.category,
    mappingKey: 'category',
    sanityQuery: `
      *[_type == "category" ${filter}] | order(title.en asc) {
        _id,
        title,
        description,
        slug
      }
    `,
    fieldMapper: mapCategoryFields,
    limit
  }, progressCallback)
}

async function syncLocations(limit = null, progressCallback = null) {
  const filter = global.SINGLE_ITEM_FILTER || ''
  return syncCollection({
    name: 'Locations',
    collectionId: WEBFLOW_COLLECTIONS.location,
    mappingKey: 'location',
    sanityQuery: `
      *[_type == "location" ${filter}] | order(name.en asc) {
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
  }, progressCallback)
}

async function syncCreators(limit = null, progressCallback = null) {
  const filter = global.SINGLE_ITEM_FILTER || ''
  return syncCollection({
    name: 'Creators',
    collectionId: WEBFLOW_COLLECTIONS.creator,
    mappingKey: 'creator',
    sanityQuery: `
      *[_type == "creator" && !(_id in path("drafts.**")) ${filter}] | order(name asc) {
        _id,
        name,
        lastName,
        cover{
          asset->{
            _id,
            url,
            originalFilename,
            _updatedAt
          },
          alt
        },
        image{
          asset->{
            _id,
            url,
            originalFilename,
            _updatedAt
          },
          alt
        },
        biography,
        portrait,
        nationality,
        specialties,
        studioImage{
          asset->{
            _id,
            url,
            originalFilename,
            _updatedAt
          },
          alt
        },
        portraitImage{
          asset->{
            _id,
            url,
            originalFilename,
            _updatedAt
          },
          alt
        },
        slug,
        website,
        email,
        birthYear,
        category
      }
    `,
    fieldMapper: mapCreatorFields,
    limit
  }, progressCallback)
}

// PHASE 8: Sync Artworks
async function syncArtworks(limit = null, progressCallback = null) {
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
        materials: materialIds,
        medium: mediumIds,
        finishes: finishIds,
        'size-dimensions': cleanSizeField(item.size || ''),
        year: item.year || '',
        price: item.price || '',
        ...(mainImage ? { 'main-image': mainImage } : {}),
        'artwork-images': artworkImages
      },
      _sanityItem: item // Store for German locale update
    }
  }

  const filter = global.SINGLE_ITEM_FILTER || ''
  return syncCollection({
    name: 'Artworks',
    collectionId: WEBFLOW_COLLECTIONS.artwork,
    mappingKey: 'artwork',
    sanityQuery: `
      *[_type == "artwork" ${filter}] | order(name asc) {
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
  }, progressCallback)
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

    // DEBUG: Log what we're actually sending for first 3 items
    if (item.name) {
      console.log(`  🔍 '${item.name.substring(0, 30)}': creator=${creatorId}`)
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
        ...(creatorId ? { creator: creatorId } : {}),
        ...(materialIds.length > 0 ? { materials: materialIds } : {}),
        ...(mediumIds.length > 0 ? { medium: mediumIds } : {}),
        ...(finishIds.length > 0 ? { finishes: finishIds } : {}),
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

// Helper function to combine alt text and caption for Webflow multi-image fields
function combineAltAndCaption(altText, caption) {
  if (altText && caption) {
    return `${altText} | ${caption}`
  }
  return altText || caption || ''
}

// PHASE 7: Sync Authors
async function syncAuthors(limit = null, progressCallback = null) {
  const filter = global.SINGLE_ITEM_FILTER || ''
  return syncCollection({
    name: 'Authors',
    collectionId: WEBFLOW_COLLECTIONS.author,
    mappingKey: 'author',
    sanityQuery: `
      *[_type == "author" ${filter}] | order(name.en asc) {
        _id,
        name,
        bio,
        slug
      }
    `,
    fieldMapper: (item, locale = 'en') => {
      const isGerman = locale === 'de-DE' || locale === 'de'
      return {
        name: isGerman ? (item.name?.de || item.name?.en || 'Untitled') : (item.name?.en || item.name?.de || 'Untitled'),
        slug: item.slug?.current || generateSlug(item.name?.en || item.name?.de),
        bio: convertSanityBlocksToWebflowRichText(isGerman ? item.bio?.de : item.bio?.en)
      }
    },
    limit
  }, progressCallback)
}

// PHASE 8: Sync Photographers
async function syncPhotographers(limit = null, progressCallback = null) {
  const filter = global.SINGLE_ITEM_FILTER || ''
  return syncCollection({
    name: 'Photographers',
    collectionId: WEBFLOW_COLLECTIONS.photographer,
    mappingKey: 'photographer',
    sanityQuery: `
      *[_type == "photographer" ${filter}] | order(name.en asc) {
        _id,
        name,
        bio,
        slug
      }
    `,
    fieldMapper: (item, locale = 'en') => {
      const isGerman = locale === 'de-DE' || locale === 'de'
      return {
        name: isGerman ? (item.name?.de || item.name?.en || 'Untitled') : (item.name?.en || item.name?.de || 'Untitled'),
        slug: item.slug?.current || generateSlug(item.name?.en || item.name?.de),
        bio: convertSanityBlocksToWebflowRichText(isGerman ? item.bio?.de : item.bio?.en)
      }
    },
    limit
  }, progressCallback)
}

// PHASE 9: Sync Articles
async function syncArticles(limit = null, progressCallback = null) {
  const filter = global.SINGLE_ITEM_FILTER || ''
  
  // Custom article mapper with image handling
  const articleCustomSync = async (item, locale = 'en') => {
    const isGerman = locale === 'de-DE' || locale === 'de'
    // Helper to prepare images (alt text from asset metadata)
    const prepareImages = (images, locale = 'en') => {
      if (!images || !Array.isArray(images)) return []
      
      return images.map(img => {
        if (!img.asset?.url) return null
        
        // Get alt text from asset metadata
        const alt = img.asset?.altText || img.asset?.alt || ''
        
        return {
          url: img.asset.url,
          alt: alt
        }
      }).filter(Boolean)
    }
    
    // Helper to prepare single image (alt text from asset metadata)
    const prepareSingleImage = (image, locale = 'en') => {
      if (!image?.asset?.url) return undefined
      return {
        url: image.asset.url,
        alt: image.asset?.altText || image.asset?.alt || ''
      }
    }
    
    // Parse fullText to extract section texts (body only, no headlines)
    const parseFullText = (fullTextBlocks) => {
      if (!fullTextBlocks || !Array.isArray(fullTextBlocks)) {
        return [null, null, null, null]
      }
      
      const sections = [[], [], [], []]
      let currentSection = 0
      
      for (const block of fullTextBlocks) {
        // Image marker - extract number from reference (e.g., "images1" → 1)
        if (block._type === 'imageMarker') {
          const match = block.reference?.match(/images(\d+)/)
          if (match) {
            const markerNum = parseInt(match[1], 10)
            // After images1 marker → go to section 1, etc.
            if (markerNum >= 1 && markerNum <= 4) {
              currentSection = markerNum
            }
          }
          continue
        }
        
        // Regular blocks go to current section
        if (currentSection >= 0 && currentSection < 4) {
          sections[currentSection].push(block)
        }
      }
      
      return sections.map(s => s.length > 0 ? s : null)
    }
    
    // Map references (DISABLED FOR TESTING - remove linked items)
    const creatorId = null // item.featuredCreator?._id ? idMappings.creator.get(item.featuredCreator._id) || null : null
    const materialIds = [] // item.materials?.map(mat => idMappings.material.get(mat._id)).filter(Boolean) || []
    const mediumIds = [] // item.medium?.map(med => idMappings.medium.get(med._id)).filter(Boolean) || []
    const finishIds = [] // item.finishes?.map(fin => idMappings.finish.get(fin._id)).filter(Boolean) || []
    const authorIds = [] // item.authors?.map(auth => idMappings.author.get(auth._id)).filter(Boolean) || []
    const photographerIds = [] // item.photographers?.map(phot => idMappings.photographer.get(phot._id)).filter(Boolean) || []
    
    // Parse fullText for both languages (body sections only)
    const enSections = parseFullText(item.fullText?.en)
    const deSections = parseFullText(item.fullText?.de)
    
    // Map layout options to Webflow option IDs (each section has unique IDs)
    const layoutOptionMaps = {
      section1: {
        'Full': '4a3ffd42437c7994a25dadc0f20a390d',
        'Main': '426941337c92fd490afa6c4d1983e7fc',
        'Small': 'fb19cfd3138f58994c68a524ba22198d',
        'Sticky left': 'ddf13115fb37e57c018f5433abb9fbdd',
        'Sticky right': '922aa1d13427fff0d0561eb871532384'
      },
      section2: {
        'Full': '82ddba882ec08cf517470df91d995f18',
        'Main': '18d480ec08d93d73ab052ef2bae27503',
        'Small': '0a08cd13f1006d1f88178a5392fca321',
        'Sticky left': 'acfd171e71355f63201af9876d9caa76',
        'Sticky right': 'e9f837902fde2529fc01fa694311371c'
      },
      section3: {
        'Full': '043a28f333019108a864de30d9cc206d',
        'Main': '675ea57ff8d5850d22ab806b75f06f93',
        'Small': 'ee466bd2505b42d0c7d88f636eaca235',
        'Sticky left': 'd922cc05e14cb8ecd134c4b069d2311a',
        'Sticky right': '726f6a3f224d2c769ae2145ae1f8cfba'
      },
      section4: {
        'Full': 'e44367c5ea5238e71c203182aa6c1b80',
        'Main': '73cc0adf63e794f6430a3659552d2845',
        'Small': '1ac0040d0c43519fa376a7c7f611b878',
        'Sticky left': '1b1391a295c99d56d3a29b508ed4b69c',
        'Sticky right': '995b63c9337888e88e1003fd0b2feb8e'
      }
    }
    
    // Get creator name and titles
    const creatorNameStr = item.creatorName || item.featuredCreator?.name || ''
    const titleEN = item.title?.en || item.title?.de || 'Untitled'
    const titleDE = item.title?.de || item.title?.en || 'Untitled'
    const heroHeadlineEN = titleEN // Just the title, no creator name
    const heroHeadlineDE = titleDE // Just the title, no creator name
    
    // English fieldData
    const englishFields = {
      name: titleEN,
      slug: item.slug?.current || generateSlug(`${creatorNameStr} ${titleEN}`),
      date: item.date || null,
      issue: item.issue || '',
      'creator-name': creatorNameStr, // Creator Name field
      'featured-creator': creatorId,
      materials: materialIds,
      'medium-2': mediumIds,
      finishes: finishIds,
      'author-s': authorIds,
      'photographer-s': photographerIds,
      'hero-headline': heroHeadlineEN,
      'hero-image-2': prepareSingleImage(item.heroImage, 'en'),
      intro: convertSanityBlocksToWebflowRichText(item.intro?.en || item.intro?.de),
      'section-1-images-2': prepareImages(item.section1Images, 'en'),
      'section-1-layout-3': layoutOptionMaps.section1[item.section1Layout] || layoutOptionMaps.section1['Main'],
      'section-1-text-2': convertSanityBlocksToWebflowRichText(enSections[0]),
      'section-1-captions-2': extractTextFromBlocks(item.section1Captions?.en),
      'section-2-images-2': prepareImages(item.section2Images, 'en'),
      'section-2-layout-3': layoutOptionMaps.section2[item.section2Layout] || layoutOptionMaps.section2['Main'],
      'section-2-text-2': convertSanityBlocksToWebflowRichText(enSections[1]),
      'section-2-captions-2': extractTextFromBlocks(item.section2Captions?.en),
      'section-3-images-2': prepareImages(item.section3Images, 'en'),
      'section-3-layout-3': layoutOptionMaps.section3[item.section3Layout] || layoutOptionMaps.section3['Main'],
      'section-3-text-2': convertSanityBlocksToWebflowRichText(enSections[2]),
      'section-3-captions-2': extractTextFromBlocks(item.section3Captions?.en),
      'section-4-images-2': prepareImages(item.section4Images, 'en'),
      'section-4-layout-3': layoutOptionMaps.section4[item.section4Layout] || layoutOptionMaps.section4['Main'],
      'section-4-text-2': convertSanityBlocksToWebflowRichText(enSections[3]),
      'section-4-captions-2': extractTextFromBlocks(item.section4Captions?.en),
      'section-final-image-1': prepareSingleImage(item.sectionFinalImage1, 'en')
    }
    
    // German fieldData (for separate locale update)
    const germanFields = {
      name: titleDE,
      issue: item.issue || '', // Same for both locales
      'creator-name': creatorNameStr, // Creator Name field (same for both locales)
      'hero-headline': heroHeadlineDE,
      'hero-image-2': prepareSingleImage(item.heroImage, 'de'),
      intro: convertSanityBlocksToWebflowRichText(item.intro?.de || item.intro?.en),
      'section-1-images-2': prepareImages(item.section1Images, 'de'),
      'section-1-text-2': convertSanityBlocksToWebflowRichText(deSections[0]),
      'section-1-captions-2': extractTextFromBlocks(item.section1Captions?.de),
      'section-2-images-2': prepareImages(item.section2Images, 'de'),
      'section-2-text-2': convertSanityBlocksToWebflowRichText(deSections[1]),
      'section-2-captions-2': extractTextFromBlocks(item.section2Captions?.de),
      'section-3-images-2': prepareImages(item.section3Images, 'de'),
      'section-3-text-2': convertSanityBlocksToWebflowRichText(deSections[2]),
      'section-3-captions-2': extractTextFromBlocks(item.section3Captions?.de),
      'section-4-images-2': prepareImages(item.section4Images, 'de'),
      'section-4-text-2': convertSanityBlocksToWebflowRichText(deSections[3]),
      'section-4-captions-2': extractTextFromBlocks(item.section4Captions?.de),
      'section-final-image-1': prepareSingleImage(item.sectionFinalImage1, 'de')
    }
    
    // Filter out undefined values
    Object.keys(englishFields).forEach(key => {
      if (englishFields[key] === undefined) delete englishFields[key]
    })
    Object.keys(germanFields).forEach(key => {
      if (germanFields[key] === undefined) delete germanFields[key]
    })
    
    // If called with German locale, return German fields only
    // Otherwise return English fields with German as secondary data
    if (isGerman) {
      return germanFields
    } else {
      return {
        fieldData: englishFields,
        germanFieldData: germanFields,
        _sanityItem: item
      }
    }
  }
  
  return syncCollection({
    name: 'Articles',
    collectionId: WEBFLOW_COLLECTIONS.article,
    mappingKey: 'article',
    sanityQuery: `
      *[_type == "article" && !(_id in path("drafts.**")) ${filter}] | order(date desc) {
        _id,
        creatorName,
        title,
        slug,
        date,
        issue,
        featuredCreator->{_id, name},
        authors[]->{_id},
        photographers[]->{_id},
        materials[]->{_id},
        medium[]->{_id},
        finishes[]->{_id},
        intro,
        fullText,
        heroImage{
          asset->{_id, url, originalFilename, altText, alt, metadata, _updatedAt}
        },
        section1Images[]{
          asset->{_id, url, originalFilename, altText, alt, metadata, _updatedAt}
        },
        section1Layout,
        section1Captions,
        section2Images[]{
          asset->{_id, url, originalFilename, altText, alt, metadata, _updatedAt}
        },
        section2Layout,
        section2Captions,
        section3Images[]{
          asset->{_id, url, originalFilename, altText, alt, metadata, _updatedAt}
        },
        section3Layout,
        section3Captions,
        section4Images[]{
          asset->{_id, url, originalFilename, altText, alt, metadata, _updatedAt}
        },
        section4Layout,
        section4Captions,
        sectionFinalImage1{
          asset->{_id, url, originalFilename, altText, alt, metadata, _updatedAt}
        }
      }
    `,
    fieldMapper: null,
    customImageSync: articleCustomSync,
    limit
  }, progressCallback)
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
        progress: {
          phase: step,
          message,
          current: currentCount,
          total: totalCount
        },
        totalSynced
      })
    }
  }
  
  const emitPhaseComplete = (phaseName) => {
    if (progressCallback) {
      progressCallback({
        phase: phaseName
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
    updateProgress('Phase 1', 'Starting foundation data sync...', 0, 6)
    console.log('\n📋 PHASE 1: Foundation Data')
    
    const phase1All = [
      { name: 'Material Types', key: 'materialType', func: syncMaterialTypes },
      { name: 'Finishes', key: 'finish', func: syncFinishes },
      { name: 'Mediums', key: 'category', func: syncCategories },
      { name: 'Locations', key: 'location', func: syncLocations },
      { name: 'Authors', key: 'author', func: syncAuthors },
      { name: 'Photographers', key: 'photographer', func: syncPhotographers }
    ]
    const syncFunctions = (only ? phase1All.filter(p => p.key === only || normalize(p.name) === normalize(only)) : phase1All)
    
    for (let i = 0; i < syncFunctions.length; i++) {
      const { name, func } = syncFunctions[i]
      try {
        updateProgress('Phase 1', `Syncing ${name}...`, i + 1, 6)
        totalSynced += await func(limitPerCollection, progressCallback)
      } catch (error) {
        console.error(`❌ Failed to sync ${name}: ${error.message}`)
        updateProgress('Phase 1', `Failed to sync ${name}: ${error.message}`, i + 1, 6)
        // Continue with other collections instead of failing completely
      }
    }
    
    emitPhaseComplete('Foundation Data')
    
    // Phase 2: Reference data (with dependencies)
    updateProgress('Phase 2', 'Starting reference data sync...', 0, 3)
    console.log('\n🔗 PHASE 2: Reference Data')
    
    const phase2All = [
      { name: 'Materials', key: 'material', func: syncMaterials },
      { name: 'Types', key: 'medium', func: syncMediums },
      { name: 'Creators', key: 'creator', func: syncCreators }
    ]
    const syncFunctions2 = (only ? phase2All.filter(p => p.key === only || normalize(p.name) === normalize(only)) : phase2All)
    
    for (let i = 0; i < syncFunctions2.length; i++) {
      const { name, func } = syncFunctions2[i]
      try {
        updateProgress('Phase 2', `Syncing ${name}...`, i + 1, 3)
        totalSynced += await func(limitPerCollection, progressCallback)
      } catch (error) {
        console.error(`❌ Failed to sync ${name}: ${error.message}`)
        updateProgress('Phase 2', `Failed to sync ${name}: ${error.message}`, i + 1, 3)
        // Continue with other collections instead of failing completely
      }
    }
    
    emitPhaseComplete('Reference Data')
    
    // Phase 3: Complex data (with multiple dependencies)
    updateProgress('Phase 3', 'Starting complex content sync...', 0, 2)
    console.log('\n🎨 PHASE 3: Complex Data')
    
    const phase3All = [
      { name: 'Artworks', key: 'artwork', func: syncArtworks },
      { name: 'Articles', key: 'article', func: syncArticles }
    ]
    const syncFunctions3 = (only ? phase3All.filter(p => p.key === only || normalize(p.name) === normalize(only)) : phase3All)
    
    for (let i = 0; i < syncFunctions3.length; i++) {
      const { name, func } = syncFunctions3[i]
      try {
        updateProgress('Phase 3', `Syncing ${name} with Images...`, i + 1, 2)
        totalSynced += await func(limitPerCollection, progressCallback)
      } catch (error) {
        console.error(`❌ Failed to sync ${name}:`, error)
        updateProgress('Phase 3', `Failed to sync ${name}: ${error.message}`, i + 1, 2)
      }
    }
    
    emitPhaseComplete('Complex Data')
    
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
    
    emitPhaseComplete('Reverse Linkage')
    
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

// ═══════════════════════════════════════════════════════════════════════════════
// SINGLE ITEM SYNC
// ═══════════════════════════════════════════════════════════════════════════════
async function syncSingleItem(documentId, documentType, autoPublish = true) {
  console.log(`\n🔍 Syncing single item: ${documentType}/${documentId}`)
  console.log('⏳ Fetching from Sanity...')
  
  // Initialize
  WEBFLOW_COLLECTIONS = await resolveWebflowCollections()
  await resolveWebflowLocales()
  await loadIdMappings()
  loadPersistentMappings()
  
  // Set global filter for targeted query
  const baseId = documentId.replace('drafts.', '')
  global.SINGLE_ITEM_FILTER = `&& (_id == "${baseId}" || _id == "drafts.${baseId}")`
  
  try {
    // Map document type to sync function
    const syncFunctions = {
      creator: () => syncCreators(1),
      artwork: () => syncArtworks(1),
      article: () => syncArticles(1),
      author: () => syncAuthors(1),
      photographer: () => syncPhotographers(1),
      category: () => syncCategories(1),
      medium: () => syncMediums(1),
      material: () => syncMaterials(1),
      materialType: () => syncMaterialTypes(1),
      finish: () => syncFinishes(1),
      location: () => syncLocations(1)
    }
    
    const syncFn = syncFunctions[documentType]
    if (!syncFn) {
      throw new Error(`Unsupported document type: ${documentType}`)
    }
    
    // Run the sync for this single item
    await syncFn()
    
    // Publish if requested
    if (autoPublish) {
      const collectionId = WEBFLOW_COLLECTIONS[documentType]
      const webflowId = idMappings[documentType]?.get(baseId)
      if (webflowId && collectionId) {
        console.log(`  📤 Publishing ${documentType}/${baseId} (${webflowId})`)
        await publishWebflowItems(collectionId, [webflowId])
      } else {
        console.log(`  ⚠️  No webflowId found for ${documentType}:${baseId}`)
      }
    }
    
    // Save mappings
    await saveIdMappings()
    
    return {
      documentId: baseId,
      documentType,
      webflowId: idMappings[documentType]?.get(baseId),
      published: autoPublish
    }
  } finally {
    // Clean up global filter
    delete global.SINGLE_ITEM_FILTER
  }
}

// Main API handler
module.exports = async function handler(req, res) {
  // CORS headers - allow all necessary headers (echo requested headers for preflight)
  res.setHeader('Access-Control-Allow-Origin', '*')
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
  const requested = req.headers['access-control-request-headers']
  if (requested) {
    res.setHeader('Access-Control-Allow-Headers', requested)
  } else {
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, content-type, Authorization')
  }
  res.setHeader('Access-Control-Expose-Headers', 'Content-Type, Authorization')
  
  if (req.method === 'OPTIONS') {
    // Preflight request
    return res.status(200).end()
  }
  
  // Support GET for streaming (no preflight CORS issues)
  if (req.method === 'GET' && (req.query?.stream === '1' || req.query?.stream === 'true')) {
    res.setHeader('Content-Type', 'text/event-stream')
    res.setHeader('Cache-Control', 'no-cache')
    res.setHeader('Connection', 'keep-alive')
    
    const sendEvent = (data) => {
      res.write(`data: ${JSON.stringify(data)}\n\n`)
    }
    
    try {
      console.log('🔔 Sync triggered via API (GET streaming)')
      const result = await performCompleteSync(sendEvent, { limitPerCollection: null })
      sendEvent({ 
        complete: true, 
        duration: result.duration, 
        totalItems: result.totalSynced 
      })
    } catch (error) {
      sendEvent({ type: 'error', error: error.message })
    } finally {
      res.end()
    }
    return
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
    
    // Check for single-item sync
    const { syncType, documentId, documentType, autoPublish, streaming, limit, limitPerCollection } = req.body || {}
    
    if (syncType === 'single-item' && documentId && documentType) {
      console.log(`🔔 Single item sync: ${documentType}/${documentId}`)
      const result = await syncSingleItem(documentId, documentType, autoPublish !== false)
      return res.status(200).json({
        success: true,
        message: `Successfully synced ${documentType}`,
        ...result
      })
    }
    
    // Check if client wants streaming progress and optional limit
    const limitValue = Number.isFinite(Number(limitPerCollection)) ? Number(limitPerCollection) : (Number.isFinite(Number(limit)) ? Number(limit) : (process.env.LIMIT_PER_COLLECTION ? Number(process.env.LIMIT_PER_COLLECTION) : null))
    
    if (streaming) {
      // Set up Server-Sent Events
      res.setHeader('Content-Type', 'text/event-stream')
      res.setHeader('Cache-Control', 'no-cache')
      res.setHeader('Connection', 'keep-alive')
      
      const sendProgress = (progress) => {
        // Send progress update
        res.write(`data: ${JSON.stringify(progress)}\n\n`)
      }
      
      try {
        console.log('🔔 Sync triggered via API (streaming)')
        const result = await performCompleteSync(sendProgress, { limitPerCollection: limitValue })
        res.write(`data: ${JSON.stringify({ 
          complete: true, 
          duration: result.duration, 
          totalItems: result.totalSynced 
        })}\n\n`)
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
  
  // Check for single item sync: node sync-article-test.js <documentId> <documentType> [--force]
  const documentId = process.argv[2]
  const documentType = process.argv[3]
  const forceUpdate = process.argv.includes('--force')
  
  if (documentId && documentType) {
    console.log(`\n🎯 Single item mode: syncing ${documentType}/${documentId}`)
    if (forceUpdate) {
      console.log('⚠️  FORCE MODE: Bypassing hash check')
      global.FORCE_UPDATE = true
    }
    syncSingleItem(documentId, documentType, true).then((result) => {
      console.log('✅ Single item sync completed!', result)
      process.exit(0)
    }).catch((error) => {
      console.error('❌ Single item sync failed:', error.message)
      console.error(error.stack)
      process.exit(1)
    })
  } else {
    // Full sync mode
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
  }
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