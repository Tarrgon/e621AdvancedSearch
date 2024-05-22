const express = require("express")
const router = express.Router()

let utils = null

async function handlePostSearch(req, res) {
  let { query, q, limit, page, reverse, excludeids: excludeIds, excludemd5s: excludeMd5s } = (req.query || {})
  if (query) query = decodeURIComponent(query)
  if (q) q = decodeURIComponent(q)
  if (excludeIds) excludeIds = excludeIds.split(",").filter(id => !isNaN(id))
  if (excludeMd5s) excludeMd5s = excludeMd5s.split(",")
  
  let { searchAfter } = (req.body || {})

  if (!query && !q && req.body.query) {
    query = req.body.query
  }

  if (!excludeIds && req.body.excludeIds) {
    excludeIds = req.body.excludeIds.filter(id => !isNaN(id)).map(id => id.toString())
  }

  if (!excludeMd5s && req.body.excludeMd5s) {
    excludeMd5s = req.body.excludeMd5s
  }

  if (limit != null && !isNaN(limit)) {
    limit = parseInt(limit)

    if (isNaN(limit)) limit = null
    else if (limit <= 0) limit = 1
    else if (limit > 320) limit = 320
  }

  if (reverse != null) {
    reverse = reverse.toLowerCase() == "true"
  } else {
    reverse = false
  }

  try {
    let result = await utils.performSearch(query ? query : q, limit ? limit : 50, searchAfter ? searchAfter : parseInt(page), reverse, excludeIds || [], excludeMd5s || [])
    // console.log(JSON.stringify(result))

    if (result.status) {
      return res.status(result.status).json(result)
    }

    return res.json(result)
  } catch (e) {
    console.error(e)

    if (!e.status) return res.sendStatus(500)
    return res.sendStatus(e.status)
  }
}

router.get("/", handlePostSearch)
router.post("/", handlePostSearch)

async function handleDefine(req, res) {
  let { query, q, limit, page, reverse } = (req.query || {})
  if (query) query = decodeURIComponent(query)
  if (q) q = decodeURIComponent(q)

  let { searchAfter } = (req.body || {})

  if (!query && !q && req.body.query) {
    query = req.body.query
  }

  if (limit != null && !isNaN(limit)) {
    limit = parseInt(limit)

    if (isNaN(limit)) limit = null
    else if (limit <= 0) limit = 1
    else if (limit > 320) limit = 320
  }

  if (reverse != null) {
    reverse = reverse.toLowerCase() == "true"
  } else {
    reverse = false
  }

  try {
    let result = await utils.defineSearch(query ? query : q, limit ? limit : 50, searchAfter ? searchAfter : parseInt(page), reverse)
    // console.log(JSON.stringify(result))
    return res.json(result)
  } catch (e) {
    console.error(e)

    if (!e.status) return res.sendStatus(500)
    return res.sendStatus(e.status)
  }
}

router.get("/define", handleDefine)
router.post("/define", handleDefine)

async function handleCountSearch(req, res) {
  let { query, q } = (req.query || {})

  if (!query && !q && req.body.query) {
    query = req.body.query
  }

  try {
    let result = await utils.countSearch(query ? query : q)
    // console.log(JSON.stringify(result))
    return res.json({ count: result })
  } catch (e) {
    console.error(e)

    if (!e.status) return res.sendStatus(500)
    return res.sendStatus(e.status)
  }
}

router.get("/count", handleCountSearch)
router.post("/count", handleCountSearch)

async function handleTagSearch(req, res) {
  let tags = req.body && req.body.tags ? req.body.tags : req.query?.tags?.split(" ")

  if (!tags || tags.length <= 0) return res.status(400).send("Tag query not present")

  try {
    let result = await utils.getTagsWithNames(tags)

    return res.json(result)
  } catch (e) {
    console.error(e)

    return res.sendStatus(500)
  }
}

router.get("/tags", handleTagSearch)
router.post("/tags", handleTagSearch)

router.get("/tagrelationships", async (req, res) => {
  let include = req.query.include ? req.query.include.split(",") : ["children", "parents"]
  let tags = req.query.tags.split(" ")

  if (tags.length > 150) tags.length = 150

  let relationships = {}

  if (include.includes("allparents")) {
    for (let tag of tags) {
      if (tag.trim() == "") continue
      let alias = await utils.getTagAliasByName(tag)

      if (alias) {
        tag = (await utils.getOrAddTagById(alias.consequentId)).name
      }

      relationships[tag] = await utils.getAllParentRelationships(tag.trim())
    }
  } else {
    for (let tag of tags) {
      if (tag.trim() == "") continue
      let alias = await utils.getTagAliasByName(tag)

      if (alias) {
        tag = (await utils.getOrAddTagById(alias.consequentId)).name
      }

      relationships[tag] = await utils.getDirectTagRelationships(tag.trim(), include)
    }
  }

  return res.json(relationships)
})

async function handleTagAliases(req, res) {
  let { query, q } = (req.query || {})

  if (!query && !q && req.body.query) {
    query = req.body.query
  }

  try {
    let result = await utils.resolveAliases(query ? query : q)

    return res.send(result)
  } catch (e) {
    console.error(e)

    return res.sendStatus(500)
  }
}

router.get("/resolvealiases", handleTagAliases)
router.post("/resolvealiases", handleTagAliases)

module.exports = (u) => {
  utils = u
  return router
}
