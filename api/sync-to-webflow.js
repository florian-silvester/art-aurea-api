const {createClient} = require('@sanity/client')
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

// Webflow site ID
const WEBFLOW_SITE_ID = '68664367794a916bfa6d247c'

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

// Collection-specific mapping functions
function mapMaterialTypeFields(sanityItem) {
  return {
    'name-english': sanityItem.name?.en || '',
    'name-german': sanityItem.name?.de || '',
    'description-english': sanityItem.description?.en || '',
    'description-german': sanityItem.description?.de || '',
    name: sanityItem.name?.en || sanityItem.name?.de || 'Untitled',
    slug: sanityItem.slug?.current || generateSlug(sanityItem.name?.en || sanityItem.name?.de)
  }
}

function mapCategoryFields(sanityItem) {
  return {
    'title-german': sanityItem.title?.de || '',
    description: sanityItem.description?.en || sanityItem.description?.de || '',
    name: sanityItem.title?.en || sanityItem.title?.de || 'Untitled',
    slug: sanityItem.slug?.current || generateSlug(sanityItem.title?.en || sanityItem.title?.de)
  }
}

function mapCreatorFields(sanityItem) {
  return {
    'biography-english': sanityItem.biography?.en || '',
    'biography-german': sanityItem.biography?.de || '',
    'portrait-english': sanityItem.portrait?.en || '',
    'portrait-german': sanityItem.portrait?.de || '',
    name: sanityItem.name || 'Untitled',
    slug: sanityItem.slug?.current || generateSlug(sanityItem.name)
  }
}

function mapLocationFields(sanityItem) {
  return {
    name: sanityItem.name?.en || sanityItem.name?.de || 'Untitled',
    slug: sanityItem.slug?.current || generateSlug(sanityItem.name?.en || sanityItem.name?.de)
  }
}

function mapMediumFinishFields(sanityItem) {
  return {
    'name-english': sanityItem.name?.en || '',
    'name-german': sanityItem.name?.de || '',
    'description-english': sanityItem.description?.en || '',
    'description-german': sanityItem.description?.de || '',
    name: sanityItem.name?.en || sanityItem.name?.de || 'Untitled',
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

// Delete items from Webflow (with batch processing)
async function deleteWebflowItems(collectionId, itemIds) {
  const results = []
  const batchSize = 5  // Smaller batches to avoid rate limits
  
  for (let i = 0; i < itemIds.length; i += batchSize) {
    const batch = itemIds.slice(i, i + batchSize)
    console.log(`  🗑️  Deleting batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(itemIds.length/batchSize)} (${batch.length} items)`)
    
    const batchPromises = batch.map(async (itemId) => {
      let attempts = 0
      while (attempts < 3) {
        try {
          await webflowRequest(`/collections/${collectionId}/items/${itemId}`, {
            method: 'DELETE'
          })
          return { itemId, status: 'deleted' }
        } catch (error) {
          attempts++
          if (error.message.includes('429') && attempts < 3) {
            console.log(`  ⏳ Rate limited, retrying ${itemId} in ${attempts * 2}s...`)
            await new Promise(resolve => setTimeout(resolve, attempts * 2000))
            continue
          }
          console.warn(`  ⚠️  Failed to delete ${itemId}: ${error.message}`)
          return { itemId, status: 'error', error: error.message }
        }
      }
    })
    
    const batchResults = await Promise.allSettled(batchPromises)
    results.push(...batchResults.map(r => r.status === 'fulfilled' ? r.value : r.reason))
    
    // Longer delay between batches to avoid rate limits
    if (i + batchSize < itemIds.length) {
      await new Promise(resolve => setTimeout(resolve, 500))
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

async function uploadImageToWebflow(imageUrl, siteName, altText = null, originalFilename = null) {
  try {
    // 1. Download image from Sanity
    console.log(`  📥 Downloading: ${imageUrl}`)
    const imageBuffer = await downloadImageBuffer(imageUrl)
    
    // 2. Generate MD5 hash
    const fileHash = generateMD5Hash(imageBuffer)
    
    // 3. Use meaningful filename (prefer original, fall back to URL)
    const filename = originalFilename || imageUrl.split('/').pop().split('?')[0] || 'artwork-image.jpg'
    
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
    
    // 5. Upload to Amazon S3
    const uploadResponse = await fetch(metadataResponse.uploadUrl, {
      method: 'POST',
      headers: {
        ...metadataResponse.uploadDetails.headers,
        'Content-Type': 'application/octet-stream'
      },
      body: imageBuffer
    })
    
    if (!uploadResponse.ok) {
      throw new Error(`S3 upload failed: ${uploadResponse.status}`)
    }
    
    // 6. Set alt text if provided
    if (altText && metadataResponse.id) {
      await updateImageMetadata(metadataResponse.id, altText)
    }
    
    // 7. Return Webflow asset ID
    console.log(`  ✅ Uploaded: ${filename}${altText ? ' (with alt text)' : ''}`)
    return metadataResponse.id
    
  } catch (error) {
    console.error(`  ❌ Failed to upload image: ${error.message}`)
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
    const altText = image.alt?.en || image.alt?.de || ''
    const originalFilename = image.asset.originalFilename || ''
    const imageUrl = image.asset.url
    
    // Check if we've seen this image before
    const existingAsset = assetMappings.get(sanityAssetId)
    
    if (!existingAsset) {
      // New image - upload it
      console.log(`  📤 Uploading new image: ${originalFilename}`)
      
      const meaningfulFilename = altText 
        ? `${altText.substring(0, 50).replace(/[^a-zA-Z0-9]/g, '-')}.jpg`
        : originalFilename
        
      const assetId = await uploadImageToWebflow(
        imageUrl, 
        'artwork', 
        altText, 
        meaningfulFilename
      )
      
      if (assetId) {
        // Track this asset
        assetMappings.set(sanityAssetId, {
          webflowAssetId: assetId,
          altText: altText,
          filename: originalFilename,
          url: imageUrl,
          lastUpdated: new Date().toISOString()
        })
        
        webflowAssetIds.push(assetId)
        uploadedCount++
      }
    } else if (hasImageMetadataChanged(image, existingAsset)) {
      // Metadata changed - update without re-uploading
      console.log(`  🔄 Updating metadata for: ${originalFilename}`)
      
      const success = await updateImageMetadata(existingAsset.webflowAssetId, altText)
      if (success) {
        // Update tracking
        assetMappings.set(sanityAssetId, {
          ...existingAsset,
          altText: altText,
          filename: originalFilename,
          url: imageUrl,
          lastUpdated: new Date().toISOString()
        })
        updatedCount++
      }
      
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

// PHASE 1: Sync Material Types
async function syncMaterialTypes() {
  console.log('📋 Syncing Material Types...')
  
  // Clear existing items first
  await clearWebflowCollection(WEBFLOW_COLLECTIONS.materialType, 'Material Types')
  
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
      ...mapMaterialTypeFields(item),
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
  
  // Clear existing items first
  await clearWebflowCollection(WEBFLOW_COLLECTIONS.finish, 'Finishes')
  
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
      ...mapMediumFinishFields(item)
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
  
  // Clear existing items first
  await clearWebflowCollection(WEBFLOW_COLLECTIONS.material, 'Materials')
  
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
      ...mapMediumFinishFields(item),
      'material-type': item.materialType?._id ? idMappings.materialType.get(item.materialType._id) || null : null
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
  
  // Clear existing items first
  await clearWebflowCollection(WEBFLOW_COLLECTIONS.medium, 'Mediums')
  
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
      ...mapMediumFinishFields(item)
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

async function syncCategories() {
  console.log('📂 Syncing Categories...')
  
  // Clear existing items first
  await clearWebflowCollection(WEBFLOW_COLLECTIONS.category, 'Categories')
  
  const sanityData = await sanityClient.fetch(`
    *[_type == "category"] | order(title.en asc) {
      _id,
      title,
      description,
      slug
    }
  `)
  
  const webflowItems = sanityData.map(item => ({
    fieldData: {
      name: item.title?.en || item.title?.de || 'Untitled',
      slug: item.slug?.current || generateSlug(item.title?.en || item.title?.de),
      'name-english': item.title?.en || '',
      'name-german': item.title?.de || '',
      'description-english': item.description?.en || '',
      'description-german': item.description?.de || ''
    }
  }))
  
  const results = await createWebflowItems(WEBFLOW_COLLECTIONS.category, webflowItems)
  
  // Store mappings
  results.forEach((webflowItem, index) => {
    const sanityItem = sanityData[index]
    idMappings.category.set(sanityItem._id, webflowItem.id)
  })
  
  console.log(`✅ Categories: ${results.length} created`)
  return results.length
}

async function syncLocations() {
  console.log('📍 Syncing Locations...')
  
  // Clear existing items first
  await clearWebflowCollection(WEBFLOW_COLLECTIONS.location, 'Locations')
  
  const sanityData = await sanityClient.fetch(`
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
  `)
  
  const webflowItems = sanityData.map(item => ({
    fieldData: {
      name: item.name?.en || item.name?.de || 'Untitled',
      slug: item.slug?.current || generateSlug(item.name?.en || item.name?.de),
      'name-english': item.name?.en || '',
      'name-german': item.name?.de || '',
      'description-english': item.description?.en || '',
      'description-german': item.description?.de || '',
      'location-type': mapLocationType(item.type),
      address: item.address || '',
      'city-location': item.city?.name?.en || item.city?.name?.de || '',
      country: item.country?.name?.en || item.country?.name?.de || '',
      website: item.website || '',
      'opening-times-english': item.times?.en || '',
      'opening-times-german': item.times?.de || '',
      phone: item.phone || '',
      email: item.email || ''
    }
  }))
  
  const results = await createWebflowItems(WEBFLOW_COLLECTIONS.location, webflowItems)
  
  // Store mappings
  results.forEach((webflowItem, index) => {
    const sanityItem = sanityData[index]
    idMappings.location.set(sanityItem._id, webflowItem.id)
  })
  
  console.log(`✅ Locations: ${results.length} created`)
  return results.length
}

async function syncCreators() {
  console.log('👤 Syncing Creators...')
  
  // Clear existing items first
  await clearWebflowCollection(WEBFLOW_COLLECTIONS.creator, 'Creators')
  
  const sanityData = await sanityClient.fetch(`
    *[_type == "creator"] | order(name asc) {
      _id,
      name,
      biography,
      portrait,
      nationality,
      specialties,
      galleryImages,
      slug
    }
  `)
  
  const webflowItems = sanityData.map(item => ({
    fieldData: {
      name: item.name || 'Untitled',
      slug: item.slug?.current || generateSlug(item.name),
      'biography-english': item.biography?.en || '',
      'biography-german': item.biography?.de || '',
      'portrait-english': item.portrait?.en || '',
      'portrait-german': item.portrait?.de || '',
      'nationality-english': item.nationality?.en || '',
      'nationality-german': item.nationality?.de || '',
      'specialties-english': item.specialties?.en || '',
      'specialties-german': item.specialties?.de || ''
    }
  }))
  
  const results = await createWebflowItems(WEBFLOW_COLLECTIONS.creator, webflowItems)
  
  // Store mappings
  results.forEach((webflowItem, index) => {
    const sanityItem = sanityData[index]
    idMappings.creator.set(sanityItem._id, webflowItem.id)
  })
  
  console.log(`✅ Creators: ${results.length} created`)
  return results.length
}

// PHASE 8: Sync Artworks
async function syncArtworks() {
  console.log('🎨 Syncing Artworks...')
  
  // Clear existing items first
  await clearWebflowCollection(WEBFLOW_COLLECTIONS.artwork, 'Artworks')
  
  const sanityData = await sanityClient.fetch(`
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
  
  const webflowItems = await Promise.all(sanityData.map(async (item) => {
    const artworkImages = await syncArtworkImages(item.images)
    
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
  }))
  
  const results = await createWebflowItems(WEBFLOW_COLLECTIONS.artwork, webflowItems)
  
  console.log(`✅ Artworks: ${results.length} created`)
  return results.length
}

// Main sync function
async function performCompleteSync(progressCallback = null) {
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
    
    // Load asset mappings for incremental image sync
    await loadAssetMappings()
    
    // Clear existing mappings
    Object.values(idMappings).forEach(map => map.clear())
    
    // Phase 1: Foundation data (no dependencies)
    updateProgress('Phase 1', 'Starting foundation data sync...', 0, 4)
    console.log('\n📋 PHASE 1: Foundation Data')
    
    const syncFunctions = [
      { name: 'Material Types', func: syncMaterialTypes },
      { name: 'Finishes', func: syncFinishes },
      { name: 'Categories', func: syncCategories },
      { name: 'Locations', func: syncLocations }
    ]
    
    for (let i = 0; i < syncFunctions.length; i++) {
      const { name, func } = syncFunctions[i]
      try {
        updateProgress('Phase 1', `Syncing ${name}...`, i + 1, 4)
        totalSynced += await func()
      } catch (error) {
        console.error(`❌ Failed to sync ${name}: ${error.message}`)
        updateProgress('Phase 1', `Failed to sync ${name}: ${error.message}`, i + 1, 4)
        // Continue with other collections instead of failing completely
      }
    }
    
    // Phase 2: Reference data (with dependencies)
    updateProgress('Phase 2', 'Starting reference data sync...', 0, 3)
    console.log('\n🔗 PHASE 2: Reference Data')
    
    const syncFunctions2 = [
      { name: 'Materials', func: syncMaterials },
      { name: 'Mediums', func: syncMediums },
      { name: 'Creators', func: syncCreators }
    ]
    
    for (let i = 0; i < syncFunctions2.length; i++) {
      const { name, func } = syncFunctions2[i]
      try {
        updateProgress('Phase 2', `Syncing ${name}...`, i + 1, 3)
        totalSynced += await func()
      } catch (error) {
        console.error(`❌ Failed to sync ${name}: ${error.message}`)
        updateProgress('Phase 2', `Failed to sync ${name}: ${error.message}`, i + 1, 3)
        // Continue with other collections instead of failing completely
      }
    }
    
    // Phase 3: Complex data (with multiple dependencies)
    updateProgress('Phase 3', 'Starting artwork sync...', 0, 1)
    console.log('\n🎨 PHASE 3: Complex Data')
    
    try {
      updateProgress('Phase 3', 'Syncing Artworks with Images...', 1, 1)
      totalSynced += await syncArtworks()
    } catch (error) {
      console.error(`❌ Failed to sync Artworks: ${error.message}`)
      updateProgress('Phase 3', `Failed to sync Artworks: ${error.message}`, 1, 1)
    }
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(1)
    console.log(`\n✅ Complete sync finished in ${duration}s`)
    console.log(`📊 Total items synced: ${totalSynced}`)
    
    // Save asset mappings for future incremental syncs
    await saveAssetMappings()
    
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
    
    // Check if client wants streaming progress
    const { streaming } = req.body || {}
    
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
        const result = await performCompleteSync(sendProgress)
        res.write(`data: ${JSON.stringify({ type: 'complete', result })}\n\n`)
        res.end()
      } catch (error) {
        res.write(`data: ${JSON.stringify({ type: 'error', error: error.message })}\n\n`)
        res.end()
      }
    } else {
      // Regular sync without streaming
      console.log('🔔 Sync triggered via API')
      const result = await performCompleteSync()
      
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