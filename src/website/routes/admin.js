const express = require("express")
const router = express.Router()

let config
let utils = null

router.use(async (req, res, next) => {
  if (!req.query.key || req.query.key != config.adminPassword) {
    return res.sendStatus(401)
  }

  next()
})


router.get("/updatetag", async (req, res) => {
  await utils.requester.updateTag(req.query.tag)
  res.sendStatus(200)
})

module.exports = (u, c) => {
  utils = u
  config = c
  return router
}
