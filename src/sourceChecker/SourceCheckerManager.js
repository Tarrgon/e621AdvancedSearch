const fs = require("fs")

class SourceCheckerManager {
  constructor(utils, db) {
    this.utils = utils
    this.db = db

    this.sourceCheckers = []
    this.queue = []

    this.setup()
    this.start()
  }

  async setup() {
    for (let file of fs.readdirSync(`${__dirname}/sites`)) {
      if (!file.endsWith(".js")) continue
      let sourceChecker = require(`${__dirname}/sites/${file}`)
      this.sourceCheckers.push(new sourceChecker())
    }
  }

  async start() {
    await this.pruneRoutine()
    this.queue = (await this.db.collection("sourceChecker").find({ checked: false }).sort({ date: -1 }).toArray()) || []
    if (this.queue.length > 0) this.queueRoutine()
  }

  async pruneRoutine() {
    let pruneDate = new Date(Date.now() - 2592000000)
    await this.db.collection("sourceChecker").deleteMany({ createdAt: { $lte: pruneDate } })

    if (this.queue.length > 0) {
      for (let i = this.queue.length; i >= 0; i--) {
        if (this.queue[i] && this.queue[i].createdAt <= pruneDate) {
          this.queue.splice(i, 1)
        }
      }
    }

    setTimeout(this.pruneRoutine.bind(this), 3600000)
  }

  hasAnySupportedSources(post) {
    for (let source of post.sources) {
      for (let sourceChecker of this.sourceCheckers) {
        if (sourceChecker.supportsSource(source)) return true
      }
    }

    return false
  }

  isSupportedSource(source) {
    for (let sourceChecker of this.sourceCheckers) {
      if (sourceChecker.supportsSource(source)) return true
    }

    return false
  }

  async queuePosts(posts) {
    let postsToQueue = []

    for (let post of posts) {
      if (!post.isPending || post.isDeleted || post.sources.length == 0 || !this.hasAnySupportedSources(post)) {
        let index = -1
        if ((index = this.queue.findIndex(p => p._id == post.id)) != -1) this.queue.splice(index, 1)
        continue
      }

      let current
      if ((current = await this.db.collection("sourceChecker").findOne({ _id: post.id })) != null) {

        let allSourcesChecked = post.sources.every(s => !this.isSupportedSource(s) || current.data?.[s] != null)

        if (allSourcesChecked) continue

        if (current.checked) await this.db.collection("sourceChecker").updateOne({ _id: post.id }, { $set: { checked: false, sources: post.sources } })

        let toQueue = {
          _id: post.id,
          createdAt: post.createdAt,
          sources: post.sources,
          width: post.width,
          height: post.height,
          fileType: post.fileType,
          fileSize: post.fileSize,
          md5: post.md5,
          checked: false
        }

        let index = -1
        if ((index = this.queue.findIndex(p => p._id == post.id)) != -1) {
          this.queue[index] = toQueue
        } else {
          this.queue.push(toQueue)
        }

        continue
      }

      let toQueue = {
        _id: post.id,
        createdAt: post.createdAt,
        sources: post.sources,
        width: post.width,
        height: post.height,
        fileType: post.fileType,
        fileSize: post.fileSize,
        md5: post.md5,
        checked: false
      }

      postsToQueue.push(toQueue)
    }

    if (postsToQueue.length == 0) {
      if (!this.queueRunning) this.queueRoutine()
      return
    }

    await this.db.collection("sourceChecker").insertMany(postsToQueue)

    this.queue.push(...postsToQueue)

    if (!this.queueRunning) this.queueRoutine()
  }

  async queueRoutine() {
    if (this.queue.length == 0) {
      this.queueRunning = false
      return
    }

    this.queueRunning = true

    let post = this.queue.shift()
    console.log("Beginning processing")
    let data = await this.processPost(post)
    await this.db.collection("sourceChecker").updateOne({ _id: post._id }, { $set: { checked: true, data } })
    console.log(`Processed. Remaining: ${this.queue.length}`)
    setTimeout(() => {
      this.queueRoutine()
    }, 2000)
  }

  async processPost(post) {
    let combinedData = {}

    let current = await this.db.collection("sourceChecker").findOne({ _id: post._id })

    if (current?.data) combinedData = current.data

    for (let sourceChecker of this.sourceCheckers) {
      let data = await sourceChecker.processPost(post, current)
      for (let [key, value] of Object.entries(data)) {
        combinedData[key] = value
      }
    }

    return combinedData
  }

  async checkFor(id) {
    try {
      let data = await this.db.collection("sourceChecker").findOne({ _id: parseInt(id) })

      if (!data || !data.data) {
        let post = await this.utils.getPost(id)
        if (!post) return { notIndexed: true, notPending: true }
        if (!post.isPending) return { notPending: true }
        if (post.isDeleted || post.sources.length == 0 || !this.hasAnySupportedSources(post)) return { unsupported: true }

        let index = -1
        if ((index = this.queue.findIndex(p => p._id == id)) == -1) {
          await this.queuePosts([post])
        }

        return { queued: true }
      }

      return data.data
    } catch (e) {
      console.error(e)
      return {}
    }
  }
}

module.exports = SourceCheckerManager