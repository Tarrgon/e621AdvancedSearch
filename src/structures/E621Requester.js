const { Readable } = require("stream")
const { finished } = require("stream/promises")
const fs = require("fs")
const gunzip = require("gunzip-file")

function wait(ms) {
  return new Promise(r => setTimeout(r, ms))
}

class E621Requester {
  static BASE_URL = "https://e621.net"
  static USER_AGENT = "E621 Advanced Search/1.0 (by DefinitelyNotAFurry4)"

  constructor(utils) {
    this.lastRequestTime = 0
    this.queued = 0
    this.utilities = utils
    this.updated = 0
  }

  makeRequest(path) {
    return new Promise(async (resolve, reject) => {
      try {
        let waitTime = 1050 - (Date.now() - this.lastRequestTime)
        this.queued++
        if (waitTime > 0) await wait(waitTime * this.queued)
        this.lastRequestTime = Date.now()
        // let headers = {}
        // if (config.login.username != "" && config.login.apiKey != "") {
        //   headers.Authorization = `Basic ${btoa(`${config.login.username}:${config.login.apiKey}`)}`
        // }

        let res = await fetch(E621Requester.BASE_URL + `/${path}&_client=${E621Requester.USER_AGENT}`)

        if (res.status == 501) {
          // Sometimes this can just happen, all this will do is cancel whatever is going on without any extra text
          return reject({ e621Moment: true })
        }

        this.queued--

        if (res.ok) {
          return resolve(await res.json())
        } else {
          return reject({ code: res.status, url: E621Requester.BASE_URL + `/${path}&_client=${E621Requester.USER_AGENT}`, text: await res.text() })
        }
      } catch (e) {
        return reject({ code: 500, url: "Fetch failed" })
      }

    })
  }

  async addNewPosts() {
    try {
      let latestPostId = await this.utilities.getLatestPostId()
      console.log(`Adding posts after ${latestPostId}`)
      let data = await this.makeRequest(`posts.json?tags=status:anylimit=320&page=a${latestPostId}`)

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
        else if (new Date(existingPost.updatedAt).getTime() != newPost.updatedAt.getTime()) await this.utilities.updatePost(newPost)
      }

      if (data.posts.length >= 320) await this.addNewPosts()
    } catch (e) {
      console.error(e)

      if (e.e621Moment == true || e.code == 500) {
        return false
      }
    }
  }

  async checkForMisses(page = 1, endPageWithNoUpdates = 10) {
    try {
      let data = await this.makeRequest(`posts.json?tags=order:change%20status:any&limit=320&page=${page}`)

      let anyUpdated = page < endPageWithNoUpdates

      let ids = []

      for (let post of data.posts) {
        if (!post.hasOwnProperty("id") ||
          !post.hasOwnProperty("file") ||
          !post.hasOwnProperty("preview") ||
          !post.hasOwnProperty("created_at") ||
          !post.hasOwnProperty("score") ||
          !post.hasOwnProperty("tags")) continue

        ids.push(post.id)
      }

      let existingPosts = await this.utilities.getPostsWithIds(ids)

      let promises = []

      let batch = { update: [], create: [] }

      for (let existingPost of existingPosts) {
        promises.push(new Promise(async (resolve) => {
          let post = data.posts.splice(data.posts.findIndex(_p => _p.id == existingPost.id), 1)[0]

          let p = await this.utilities.createPost(post.id, post.tags, post.uploader_id, post.approver_id, post.created_at, post.updated_at,
            post.file.md5, post.sources, post.rating, post.file.width, post.file.height, post.duration || 0, post.fav_count, post.score.total,
            post.relationships.parent_id, post.relationships.children, post.file.ext, post.file.size, post.comment_count, post.flags.deleted,
            post.flags.pending, post.flags.flagged, post.flags.rating_locked, post.flags.status_locked, post.flags.note_locked)

          if (p.isDeleted != existingPost.isDeleted || p.isFlagged != existingPost.isFlagged || p.isPending != existingPost.isPending || p.md5 != existingPost.md5) {
            console.log(`Updating missed post: ${post.id}`)

            anyUpdated = true

            batch.update.push(p)
          }

          resolve()
        }))
      }

      if (data.posts.length > 0) {
        anyUpdated = true
      }

      for (let post of data.posts) {
        promises.push(new Promise(async (resolve) => {
          let p = await this.utilities.createPost(post.id, post.tags, post.uploader_id, post.approver_id, post.created_at, post.updated_at,
            post.file.md5, post.sources, post.rating, post.file.width, post.file.height, post.duration || 0, post.fav_count, post.score.total,
            post.relationships.parent_id, post.relationships.children, post.file.ext, post.file.size, post.comment_count, post.flags.deleted,
            post.flags.pending, post.flags.flagged, post.flags.rating_locked, post.flags.status_locked, post.flags.note_locked)

          console.log(`Adding missed post: ${post.id}`)

          batch.create.push(p)

          resolve()
        }))
      }

      await Promise.all(promises)

      await this.utilities.bulkUpdateOrAddPosts(batch)

      if (anyUpdated && page < 750) {
        // console.log(`Continuing to next page of misses: ${page + 1}`)
        await this.checkForMisses(page + 1)
      }
    } catch (e) {
      console.error(e)

      if (e.e621Moment == true || e.code == 500) {
        return false
      }
    }
  }

  async applyUpdates(page = 1, endPageWithNoUpdates = 10) {
    try {
      let anyUpdated = page < endPageWithNoUpdates

      let data = await this.makeRequest(`posts.json?tags=order:updated_desc%20status:any&limit=320&page=${page}`)

      let ids = []

      for (let post of data.posts) {
        if (!post.hasOwnProperty("id") ||
          !post.hasOwnProperty("file") ||
          !post.hasOwnProperty("preview") ||
          !post.hasOwnProperty("created_at") ||
          !post.hasOwnProperty("score") ||
          !post.hasOwnProperty("tags")) continue

        ids.push(post.id)
      }

      let existingPosts = await this.utilities.getPostsWithIds(ids)

      let promises = []

      let batch = []

      for (let existingPost of existingPosts) {
        promises.push(new Promise(async (resolve) => {
          let post = data.posts.find(_p => _p.id == existingPost.id)

          if (new Date(existingPost.updatedAt).getTime() != new Date(post.updated_at).getTime()) {
            let p = await this.utilities.createPost(post.id, post.tags, post.uploader_id, post.approver_id, post.created_at, post.updated_at,
              post.file.md5, post.sources, post.rating, post.file.width, post.file.height, post.duration || 0, post.fav_count, post.score.total,
              post.relationships.parent_id, post.relationships.children, post.file.ext, post.file.size, post.comment_count, post.flags.deleted,
              post.flags.pending, post.flags.flagged, post.flags.rating_locked, post.flags.status_locked, post.flags.note_locked)

            this.updated++
            anyUpdated = true

            batch.push(p)
          }

          resolve()
        }))
      }

      await Promise.all(promises)

      await this.utilities.bulkUpdatePosts(batch)

      if (anyUpdated && page < 750) {
        // console.log(`Applying next page of updates: ${page + 1}`)
        await this.applyUpdates(page + 1)
      }
    } catch (e) {
      console.error(e)

      if (e.e621Moment == true || e.code == 500) {
        return false
      }
    }
  }

  async updateTagAliases(page = 1) {
    try {
      console.log(`Updating tag aliases`)
      let data = await this.makeRequest(`tag_aliases.json?limit=100&%5Border%5D=updated_at&page=${page}`)

      let keepGoing = true

      if (data.tag_aliases) return

      for (let tagAlias of data) {
        let existingTagAlias = await this.utilities.getTagAlias(tagAlias.id)

        // If we get to a tag that is the same, we don't need to update further
        if (existingTagAlias && existingTagAlias.updatedAt >= new Date(tagAlias.updated_at)) {
          keepGoing = false
          break
        }

        if (tagAlias.status == "active") {
          let usedTag = await this.utilities.getOrAddTag(tagAlias.consequent_name)

          if (!usedTag) {
            console.error(`Unable to add tag alias: ${tagAlias.antecedent_name} -> ${tagAlias.consequent_name}`)
            continue
          }

          if (!existingTagAlias) await this.utilities.addTagAlias({ id: tagAlias.id, antecedentName: tagAlias.antecedent_name, consequentId: usedTag.id, updatedAt: new Date(tagAlias.updated_at) })
          else if (existingTagAlias.antecedentName != tagAlias.antecedent_name || existingTagAlias.consequentId != usedTag.id) await this.utilities.updateTagAlias({ id: tagAlias.id, antecedentName: tagAlias.antecedent_name, consequentId: usedTag.id, updatedAt: new Date(tagAlias.updated_at) })
        } else {
          if (existingTagAlias) {
            await this.utilities.deleteTagAlias(tagAlias.id)
          }
        }
      }

      if (keepGoing) await this.updateTagAliases(++page)
    } catch (e) {
      console.error(e)

      if (e.e621Moment == true || e.code == 500) {
        return false
      }
    }
  }

  async updateTagImplications(page = 1) {
    try {
      console.log(`Updating tag implications`)
      let data = await this.makeRequest(`tag_implications.json?limit=100&%5Border%5D=updated_at&page=${page}`)

      let keepGoing = true

      if (data.tag_implications) return

      for (let tagImplication of data) {
        let existingTagImplication = await this.utilities.getTagImplication(tagImplication.id)

        // If we get to a tag that is the same, we don't need to update further
        if (existingTagImplication && existingTagImplication.updatedAt >= new Date(tagImplication.updated_at)) {
          keepGoing = false
          break
        }

        if (tagImplication.status == "active") {
          let child = await this.utilities.getOrAddTag(tagImplication.antecedent_name)
          let parent = await this.utilities.getOrAddTag(tagImplication.consequent_name)

          if (!child || !parent) {
            console.error(`Unable to add tag implication: ${tagImplication.antecedent_name} -> ${tagImplication.consequent_name} (${!child} | ${!parent})`)
            continue
          }

          if (!existingTagImplication) await this.utilities.addTagImplication({ id: tagImplication.id, antecedentId: child.id, consequentId: parent.id, updatedAt: new Date(tagImplication.updated_at) })
          else if (existingTagImplication.antecedentId != child.id || existingTagImplication.consequentId != parent.id) await this.utilities.updateTagImplication({ id: tagImplication.id, antecedentId: child.id, consequentId: parent.id, updatedAt: new Date(tagImplication.updated_at) })
        } else {
          if (existingTagImplication) {
            await this.utilities.deleteTagImplication(tagImplication.id)
          }
        }
      }

      if (keepGoing) await this.updateTagImplications(++page)
    } catch (e) {
      console.error(e)

      if (e.e621Moment == true || e.code == 500) {
        return false
      }
    }
  }

  async getTag(tagName) {
    try {
      console.log(`Getting new tag: "${tagName}"`)
      // let d = await this.utilities.getTagByName(tagName)
      // if (d) return d
      let data = await this.makeRequest(`tags.json?limit=1&search[name_matches]=${tagName}`)
      if (data && data[0]) {
        return { id: data[0].id, name: data[0].name, category: data[0].category }
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