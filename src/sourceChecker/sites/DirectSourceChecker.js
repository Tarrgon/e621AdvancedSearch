const SourceChecker = require("../SourceChecker")
const jsmd5 = require("js-md5")

class DirectSourceChecker extends SourceChecker {
  constructor() {
    super()

    /**
     * TODO:
     * vk.com
     * InkBunny (not direct)
     * Itaku (not direct)
     * Pixiv (not direct)
     */

    this.SUPPORTED = [
      /^https?:\/\/pbs\.twimg\.com\/media\/.*\.(png|jpg|jpeg).*/,
      /^https?:\/\/pbs\.twimg\.com\/media\/.*\?format=(png|jpg|jpeg).*/,
      /^https?:\/\/inkbunny\.net\/files\/.*\.(png|jpg|jpeg|gif).*/,
      /^https?:\/\/d\.furaffinity\.net\/art\/.*\.(png|jpg|jpeg|gif).*/,
      /^https?:\/\/media\.baraag\.net\/media_attachments\/.*\.(png|jpg|jpeg|gif).*/,
      /^https?:\/\/artconomy.com\/media\/art\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /^https?:\/\/images\.artfight\.net\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /^https?:\/\/cdn.*\.artstation\.com\/p\/assets\/images\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /^https?:\/\/derpicdn\.net\/img\/(view|download)\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /^https?:\/\/images-wixmp-.*\.wixmp\.com\/f\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /^https?:\/\/dl\.dropboxusercontent\.com\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /^https?:\/\/.*\.cloudfront\.net\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /^https?:\/\/itaku.ee\/api\/.*\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /^https?:\/\/cdn\.weasyl\.com\/.*\/submissions\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /^https?:\/\/uploads\.ungrounded\.net\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /^https?:\/\/art\.ngfiles\.com\/images\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /^https?:\/\/.*\.ib\.metapix\.net\/files\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /^https?:\/\/files\.catbox\.moe\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /^https?:\/\/i\.imgur\.com\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /^https?:\/\/.*sofurryfiles\.com\/.*\?page=(\d+).*/,
      /^https?:\/\/img\.pawoo\.net\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /^https?:\/\/pawb\.fun\/system\/media_attachments\/files\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /^https?:\/\/files\.mastodon\.social\/media_attachments\/files\/.*\.(png|jpg|jpeg|gif|webm).*/,
      /^https?:\/\/cdn\.discordapp\.com\/attachments\/.*\.(png|jpg|jpeg|gif|webm).*/,
    ]
  }

  supportsSource(source) {
    for (let supported of this.SUPPORTED) {
      if (supported.test(source)) return true
    }

    return false
  }

  async processPost(post, current) {
    let data = {}
    for (let source of post.sources) {
      if (current?.data?.[source]) continue
      if (this.supportsSource(source)) {
        data[source] = await this._processDirectLink(post, source)
      }
    }

    return data
  }
}

module.exports = DirectSourceChecker