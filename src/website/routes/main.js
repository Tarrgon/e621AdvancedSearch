const express = require("express")
const router = express.Router()

let utils = null

async function handle(req, res) {
  let { query, q, limit, page } = (req.query || {})
  let { searchAfter } = (req.body || {})

  if (!query && !q && req.body.query) {
    query = req.body.query
  }

  if (limit && !isNaN(limit)) {
    limit = parseInt(limit)

    if (isNaN(limit)) limit = null
    else if (limit <= 0) limit = 1
    else if (limit > 320) limit = 320
  }

  try {
    let result = await utils.performSearch(query ? query : q, limit ? limit : 50, searchAfter ? searchAfter : parseInt(page))
    // console.log(JSON.stringify(result))
    return res.json(result)
  } catch (e) {
    console.error(e)

    if (!e.status) return res.sendStatus(500)
    return res.status(e.status).send(e.message)
  }
}

router.get("/", handle)
router.post("/", handle)

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
        tag = (await utils.getTag(alias.consequentId)).name
      }

      relationships[tag] = await utils.getAllParentRelationships(tag.trim())
    }
  } else {
    for (let tag of tags) {
      if (tag.trim() == "") continue
      let alias = await utils.getTagAliasByName(tag)

      if (alias) {
        tag = (await utils.getTag(alias.consequentId)).name
      }

      relationships[tag] = await utils.getDirectTagRelationships(tag.trim(), include)
    }
  }

  return res.json(relationships)
})

module.exports = (u) => {
  utils = u
  return router
}
