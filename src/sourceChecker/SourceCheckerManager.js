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
    await this.db.collection("sourceChecker").deleteMany({ date: { $lte: pruneDate } })

    if (this.queue.length > 0) {
      for (let i = this.queue.length; i >= 0; i--) {
        if (this.queue[i] && this.queue[i].checkedAt <= pruneDate) {
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

  getSupportedSources(post) {
    let supported = []

    for (let source of post.sources) {
      for (let sourceChecker of this.sourceCheckers) {
        if (sourceChecker.supportsSource(source)) supported.push(source)
      }
    }

    return supported
  }

  isSupportedSource(source) {
    for (let sourceChecker of this.sourceCheckers) {
      if (sourceChecker.supportsSource(source)) return true
    }

    return false
  }

  async queuePosts(posts, force = false) {
    let postsToQueue = []

    for (let post of posts) {
      if ((!post.isPending && !force) || post.isDeleted || post.sources.length == 0 || !this.hasAnySupportedSources(post)) {
        let index = -1
        if ((index = this.queue.findIndex(p => p._id == post.id)) != -1) this.queue.splice(index, 1)
        continue
      }

      let current
      if ((current = await this.db.collection("sourceChecker").findOne({ _id: post.id })) != null) {

        let allSourcesChecked = post.sources.every(s => !this.isSupportedSource(s) || current.data?.[s] != null)

        if (allSourcesChecked) continue

        if (current.checked) await this.db.collection("sourceChecker").updateOne({ _id: post.id }, { $set: { checked: false, date: new Date(), sources: post.sources } })

        let toQueue = {
          _id: post.id,
          date: new Date(),
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
        date: new Date(),
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
    await this.db.collection("sourceChecker").updateOne({ _id: post._id }, { $set: { checked: true, date: new Date(), data } })
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

  async checkFor(id, checkApproved = false) {
    try {
      let data = await this.db.collection("sourceChecker").findOne({ _id: parseInt(id) })

      let post = await this.utils.getPost(id)

      let supportedSources = []
      if (post) supportedSources = this.getSupportedSources(post)

      if (!data || !data.data) {
        if (!post) return { notIndexed: true, notPending: true }
        if (!post.isPending && !checkApproved) return { notPending: true }
        if (post.isDeleted || post.sources.length == 0 || supportedSources.length == 0) return { unsupported: true }

        let index = -1
        if ((index = this.queue.findIndex(p => p._id == id)) == -1) {
          await this.queuePosts([post], checkApproved)
        }

        return { queued: true }
      }

      if (post && supportedSources.some(s => !data.data[s])) {
        let index = -1
        if ((index = this.queue.findIndex(p => p._id == id)) == -1) {
          await this.queuePosts([post], checkApproved)
        }
      }

      return data.data
    } catch (e) {
      console.error(e)
      return {}
    }
  }

  async checkBulk(ids, checkApproved = false) {
    try {
      ids = ids.map(id => parseInt(id))
      let allData = await this.db.collection("sourceChecker").find({ _id: { $in: ids } }).toArray()

      let posts = await this.utils.getPostsWithIds(ids)

      let returnData = []

      let toQueue = []

      for (let post of posts) {
        let supportedSources = this.getSupportedSources(post)

        ids.splice(ids.indexOf(post.id), 1)

        let data = allData.find(d => d._id == post.id)

        if (!data || !data.data) {
          if (!post.isPending && !checkApproved) {
            returnData.push({ id: post.id, notPending: true })
            continue
          }

          if (post.isDeleted || post.sources.length == 0 || supportedSources.length == 0) {
            returnData.push({ id: post.id, unsupported: true })
            continue
          }

          let index = -1
          if ((index = this.queue.findIndex(p => p._id == id)) == -1) {
            toQueue.push(post)
          }

          returnData.push({ id: post.id, queued: true })
          continue
        }

        if (supportedSources.some(s => !data.data[s])) {
          let index = -1
          if ((index = this.queue.findIndex(p => p._id == id)) == -1) {
            toQueue.push(post)
            returnData.push({ id: post.id, queued: true })
            continue
          }
        }

        returnData.push({ id: post.id, sources: data.data })
      }

      if (toQueue.length > 0) {
        await this.queuePosts(toQueue, checkApproved)
      }

      for (let id of ids) {
        returnData.push({ id, notIndexed: true, notPending: true })
      }

      return returnData
    } catch (e) {
      console.error(e)
      return {}
    }
  }
}

module.exports = SourceCheckerManager