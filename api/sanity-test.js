const {createClient} = require("@sanity/client")

const sanityClient = createClient({
  projectId: "b8bczekj",
  dataset: "production", 
  useCdn: false,
  apiVersion: "2023-01-01",
  token: process.env.SANITY_API_TOKEN
})

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*")
  
  if (req.method === "OPTIONS") return res.status(200).end()
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" })
  
  try {
    console.log("🔔 Testing Sanity connection...")
    
    // Test Sanity query
    const sanityData = await sanityClient.fetch(`
      *[_type == "materialType"][0...2] {
        _id,
        name,
        description
      }
    `)
    
    console.log("✅ Sanity data fetched:", sanityData.length, "items")
    
    return res.status(200).json({
      success: true,
      sanity_items: sanityData.length,
      sample_data: sanityData[0] || null
    })
    
  } catch (error) {
    console.error("❌ Error:", error.message)
    return res.status(500).json({ 
      error: error.message,
      stack: error.stack.split("\n").slice(0, 5)
    })
  }
}
