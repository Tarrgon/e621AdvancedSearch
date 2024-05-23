const SourceChecker = require("../SourceChecker")
const jsmd5 = require("js-md5")

function wait(ms) {
  return new Promise(r => setTimeout(r, ms))
}

class ItakuPostsSourceChecker extends SourceChecker {
  constructor() {
    super()

    this.SUPPORTED = [
      /^https?:\/\/itaku\.ee\/posts\/(\d+).*/,
      /^https?:\/\/itaku\.ee\/images\/(\d+).*/,
    ]
  }

  supportsSource(source) {
    for (let supported of this.SUPPORTED) {
      if (supported.test(source)) return true
    }

    return false
  }

  async _internalProcessPost(post, source) {
    try {
      let id = this.SUPPORTED.map(r => r.exec(source)?.[1]).filter(id => id)[0]
      if (!id) {
        console.error(`Could not find ID for: ${source} (${post._id})`)
        return {
          unknown: true,
          error: true,
          md5Match: false,
          dimensionMatch: false,
          fileTypeMatch: false
        }
      }

      let res = await fetch(`https://itaku.ee/api/galleries/images/?ids=${id}&format=json`)

      if (!res.ok) {
        console.error(`Error with (${res.status}): ${source} (${post._id})`)
        return {
          unknown: true,
          error: true,
          md5Match: false,
          dimensionMatch: false,
          fileTypeMatch: false
        }
      }

      let data = (await res.json()).results?.[0]

      if (!data) {
        console.error(`Error with (no data): ${source} (${post._id})`)
        return {
          unknown: true,
          error: true,
          md5Match: false,
          dimensionMatch: false,
          fileTypeMatch: false
        }
      }

      return await this._processDirectLink(post, data.image)
    } catch (e) {
      console.error(post._id, source)
      console.error(e)
    } finally {
      await page.close()
    }

    return {
      unknown: true,
      error: true,
      md5Match: false,
      dimensionMatch: false,
      fileTypeMatch: false
    }
  }

  async processPost(post, current) {
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

module.exports = ItakuPostsSourceChecker