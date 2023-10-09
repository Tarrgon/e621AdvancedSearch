// Dependencies
const { MongoClient } = require("mongodb")
const express = require("express")
const Utilities = require("../structures/Utilities")

const config = require("../config.json")

module.exports = async () => {
  console.log("Starting")
  try {
    const client = new MongoClient(config.mongoDatabaseUrl)
    await client.connect()
    const database = client.db(config.mongoDatabaseName)
    const utils = new Utilities(database)

    const app = express()

    app.set("trust proxy", 1)

    // routers
    app.use("/", require("./routes/main.js")(utils))

    return app
  } catch (e) {
    console.error(e)
  }
}

process.on("unhandledRejection", (r, p) => {
  console.error(r)
  console.error(p)
})