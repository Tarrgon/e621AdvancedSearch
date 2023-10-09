const { Readable } = require("stream")
const { finished } = require("stream/promises")
const fs = require("fs")
const gunzip = require("gunzip-file")

const config = require("../config.json")

function wait(ms) {
  return new Promise(r => setTimeout(r, ms))
}

class E621Requester {
  static BASE_URL = "https://e621.net"
  static USER_AGENT = "E621 Advanced Search/1.0 (by DefinitelyNotAFurry4)"

  constructor(utils) {
    this.lastRequestTime = 0
    this.utilities = utils
  }

  makeRequest(path) {
    return new Promise(async (resolve, reject) => {
      let waitTime = 1000 - (Date.now() - this.lastRequestTime)
      if (waitTime > 0) await wait(waitTime)
      this.lastRequestTime = Date.now()
      // let headers = {}
      // if (config.login.username != "" && config.login.apiKey != "") {
      //   headers.Authorization = `Basic ${btoa(`${config.login.username}:${config.login.apiKey}`)}`
      // }

      let res = await fetch(E621Requester.BASE_URL + `/${path}&_client=${E621Requester.USER_AGENT}`)

      if (res.ok) {
        return resolve(await res.json())
      } else {
        return reject({ code: res.status, url: E621Requester.BASE_URL + `/${path}&_client=${E621Requester.USER_AGENT}`, text: await res.text() })
      }
    })
  }

  async addNewPosts() {
    try {
      let latestPostId = await this.utilities.getLatestPostId()
      console.log(`Adding posts after ${latestPostId}`)
      let data = await this.makeRequest(`posts.json?limit=320&page=a${latestPostId}`)

      for (let post of data.posts) {
        if (!post.hasOwnProperty("id") ||
          !post.hasOwnProperty("file") ||
          !post.hasOwnProperty("preview") ||
          !post.hasOwnProperty("created_at") ||
          !post.hasOwnProperty("score") ||
          !post.hasOwnProperty("tags")) continue

        let newPost = await this.utilities.createPost(post.id, post.tags, post.uploader_id, post.approver_id, post.created_at, post.updated_at,
          post.file.md5, post.sources, post.rating, post.file.width, post.file.height, post.duration || 0, post.fav_count, post.score.total,
          post.relationships.parent_id, post.relationships.children, post.file.ext, post.file.size, post.comment_count, post.flags.deleted,
          post.flags.pending, post.flags.flagged, post.flags.rating_locked, post.flags.status_locked, post.flags.note_locked)

        let existingPost = await this.utilities.getPost(post.id)
        
        if (!existingPost) await this.utilities.addPost(newPost)
        else if (existingPost.updatedAt.getTime() != newPost.updatedAt.getTime()) await this.utilities.updatePost(newPost)
      }

      if (data.posts.length >= 320) await this.addNewPosts()
    } catch (e) {
      console.error(e)
    }
  }

  async applyUpdates(page = 1) {
    try {
      let data = await this.makeRequest(`posts.json?limit=320&order=change&page=${page}`)

      for (let post of data.posts) {
        if (!post.hasOwnProperty("id") ||
          !post.hasOwnProperty("file") ||
          !post.hasOwnProperty("preview") ||
          !post.hasOwnProperty("created_at") ||
          !post.hasOwnProperty("score") ||
          !post.hasOwnProperty("tags")) continue

        let p = await this.utilities.createPost(post.id, post.tags, post.uploader_id, post.approver_id, post.created_at, post.updated_at,
          post.file.md5, post.sources, post.rating, post.file.width, post.file.height, post.duration || 0, post.fav_count, post.score.total,
          post.relationships.parent_id, post.relationships.children, post.file.ext, post.file.size, post.comment_count, post.flags.deleted,
          post.flags.pending, post.flags.flagged, post.flags.rating_locked, post.flags.status_locked, post.flags.note_locked)

        let existingPost = await this.utilities.getPost(post.id)

        if (!existingPost) {
          await this.utilities.addPost(p)
        } else if (existingPost.updatedAt.getTime() == p.updatedAt.getTime()) {
          // Since it's ordered by latest update, if a post exists and matches the update time, all subsequent ones will as well.
          return
        } else {
          await this.utilities.updatePost(p)
        }
      }

      await this.applyUpdates(page + 1)
    } catch (e) {
      console.error(e)
    }
  }

  async getNewTagAliases() {
    try {
      let latestTagAliasId = await this.utilities.getLatestTagAliasId()
      console.log(`Adding tag aliases after ${latestTagAliasId}`)
      let data = await this.makeRequest(`tag_aliases.json?limit=100&page=a${latestTagAliasId}`)

      for (let tagAlias of data) {
        if (tagAlias.status == "active") {
          let existingTagAlias = await this.utilities.getTagAlias(tagAlias.id)

          let usedTag = await this.utilities.getOrAddTag(tagAlias.consequent_name)

          if (!usedTag) {
            console.error(`Unable to add tag alias: ${tagAlias.antecedent_name} -> ${tagAlias.consequent_name}`)
            continue
          }

          if (!existingTagAlias) await this.utilities.addTagAlias({ _id: tagAlias.id, antecedentName: tagAlias.antecedent_name, consequentId: usedTag._id })
          else if (existingTagAlias.antecedentName != tagAlias.antecedent_name || existingTagAlias.consequentId != usedTag._id) await this.utilities.updateTagAlias({ _id: tagAlias.id, antecedentName: tagAlias.antecedent_name, consequentId: usedTag._id })
        }
      }

      if (data.length >= 100) await this.addNewPosts()
    } catch (e) {
      console.error(e)
    }
  }

  async getTag(tagName) {
    try {
      console.log(`Getting new tag: ${tagName}`)
      let data = await this.makeRequest(`tags.json?limit=1&search[name_matches]=${tagName}`)
      if (data && data[0]) {
        return { _id: data[0].id, name: data[0].name, category: data[0].category }
      } else {
        return null
      }
    } catch (e) {
      if (e.code == 404) {
        console.error(`Tag not found: ${tagName}`)
      } else {
        console.error(e)
      }
    }
  }

  getDatabaseExport(exportName) {
    return new Promise(async (resolve, reject) => {
      if (fs.existsSync(`./${exportName.slice(0, -3)}`))
        return resolve(fs.createReadStream(`./${exportName.slice(0, -3)}`, { encoding: "utf-8" }))
      
      let res = await fetch(`https://e621.net/db_export/${exportName}`)
      if (res.ok) {
        const fileStream = fs.createWriteStream(`./${exportName}`, { flags: 'wx' })
        await finished(Readable.fromWeb(res.body).pipe(fileStream))
        gunzip(`./${exportName}`, `./${exportName.slice(0, -3)}`, () => {
          fs.rmSync(`./${exportName}`)
          resolve(fs.createReadStream(`./${exportName.slice(0, -3)}`, { encoding: "utf-8" }))
        })
      } else {
        reject({ code: res.status, text: await res.text() })
      }
    })
  }
}

module.exports = E621Requester