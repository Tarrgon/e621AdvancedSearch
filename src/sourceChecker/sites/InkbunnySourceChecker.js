const SourceChecker = require("../SourceChecker")
const jsmd5 = require("js-md5")
const config = require("../../config.json")

function wait(ms) {
  return new Promise(r => setTimeout(r, ms))
}

class InkbunnySourceChecker extends SourceChecker {
  constructor() {
    super()

    this.ready = false

    const ib = require('ib-helper')
    this.inkbunnyHelper = new ib.Helper()
    this.setup()

    this.SUPPORTED = [
      /.*:\/\/inkbunny\.net\/s\/(\d+).*/,
      /.*:\/\/inkbunny\.net\/submissionview\.php\?.*id=(\d+).*/,
    ]
  }

  async setup() {
    await this.inkbunnyHelper.login(config.inkbunny.username, config.inkbunny.password)
    this.ready = true
  }

  supportsSource(source) {
    for (let supported of this.SUPPORTED) {
      if (supported.test(source)) return true
    }

    return false
  }

  getIdFromSource(source) {
    for (let supported of this.SUPPORTED) {
      let r = supported.exec(source)
      if (r) return r[1]
    }

    return null
  }

  async _internalProcessPost(post, source) {
    try {
      let id = this.getIdFromSource(source)
      if (!id) {
        return {
          unknown: true,
          error: true,
          md5Match: false,
          dimensionMatch: false,
          fileTypeMatch: false
        }
      }

      let data = await this.inkbunnyHelper.details(id, false, false, false)
      let submission = data.submissions[0]
      if (!submission) {
        return {
          unknown: true,
          error: true,
          md5Match: false,
          dimensionMatch: false,
          fileTypeMatch: false
        }
      }

      let matchData = []

      for (let file of submission.files) {
        let fileType = SourceChecker.MIME_TYPE_TO_FILE_EXTENSION[file.mimetype]
        if (!fileType) {
          return {
            unknown: true,
            error: true,
            md5Match: false,
            dimensionMatch: false,
            fileTypeMatch: false
          }
        }

        let d = {
          md5Match: file.full_file_md5 == post.md5,
          dimensionMatch: file.full_size_x == post.width && file.full_size_y == post.height,
          fileTypeMatch: fileType == post.fileType,
          fileType: fileType,
          dimensions: {
            width: file.full_size_x,
            height: file.full_size_y
          }
        }

        d.score = (d.md5Match * 1000) + (d.dimensionMatch * 500) + d.fileTypeMatch

        matchData.push(d)
      }

      if (matchData.length > 0) {
        matchData.sort((a, b) => b.score - a.score)

        return matchData[0]
      }
    } catch (e) {
      console.error(post._id, source)
      console.error(e)
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

module.exports = InkbunnySourceChecker