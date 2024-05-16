const SourceChecker = require("../SourceChecker")
const jsmd5 = require("js-md5")
const { Scraper } = require("@DontTalkToMeThx/twitter-scraper")
const config = require("../../config.json")

function wait(ms) {
  return new Promise(r => setTimeout(r, ms))
}

class TwitterChecker extends SourceChecker {
  static NAMES = ["orig", "4096x4096", "large"]

  constructor() {
    super()

    this.scraper = new Scraper({
      fetch: fetch
    })

    this.ready = false

    this.scraper.login(config.twitterAuths).then(() => {
      this.ready = true
    })

    this.SUPPORTED = [new RegExp(".*:\/\/(x|twitter)\.com\/.*\/status\/(\d*).*")]
  }

  supportsSource(source) {
    for (let supported of this.SUPPORTED) {
      if (supported.test(source) && !source.includes("/video/")) return true
    }

    return false
  }

  async _internalProcessPost(post, source) {
    let data = (/.*:\/\/(x|twitter)\.com\/.*\/status\/(\d+).*/).exec(source)

    let id = data[2]

    if (id) {
      try {
        let tweet = await this.scraper.getTweet(id)
        if (tweet) {
          let matchData = []
          for (let photo of tweet.photos) {
            for (let name of TwitterChecker.NAMES) {
              let res = await fetch(photo.url + `?name=${name}`)
              let blob = await res.blob()
              let arrayBuffer = await blob.arrayBuffer()

              let md5 = jsmd5(arrayBuffer)

              let dimensions = await super.getDimensions(blob.type, arrayBuffer)

              let d = {
                md5Match: md5 == post.md5,
                dimensionMatch: dimensions.width == post.width && dimensions.height == post.height,
                fileTypeMatch: SourceChecker.MIME_TYPE_TO_FILE_EXTENSION[blob.type] == post.fileType,
                fileType: SourceChecker.MIME_TYPE_TO_FILE_EXTENSION[blob.type],
                dimensions
              }

              d.score = (d.md5Match * 1000) + (d.dimensionMatch * 500) + d.fileTypeMatch

              matchData.push(d)
            }
          }

          if (matchData.length > 0) {
            matchData.sort((a, b) => b.score - a.score)

            return matchData[0]
          }
        }
      } catch (e) {
        console.error(e)
        if (e.message == "Exhausted") return await this._internalProcessPost(post, source)
      }
    }

    return {
      md5Match: false,
      dimensionMatch: false,
      fileTypeMatch: false
    }
  }

  async processPost(post, current) {
    while (!this.ready) {
      await wait(500)
    }

    let data = {}
    for (let source of post.sources) {
      if (current?.data?.[source]) continue
      if (this.supportsSource(source)) {
        data[source] = await this._internalProcessPost(post, source)
      }
    }

    return data
  }
}

module.exports = TwitterChecker