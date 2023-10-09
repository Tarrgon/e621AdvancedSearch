const E621Requester = require("./E621Requester.js")
const Tokenizer = require("./Tokenizer.js")
const fs = require("fs")
const csv = require("csv-parse")

const TOKENS_TO_SKIP = ["~", "^", "-"]
const MODIFIERS = {
  NONE: 0,
  OR: 1,
  EXCLUSIVE_OR: 2,
  NOT: 3
}

function wait(ms) {
  return new Promise(r => setTimeout(r, ms))
}

class Utilities {
  static singleton

  constructor(database) {
    this.database = database

    this.database.collection("tags").createIndex({ name: 1 })
    this.database.collection("tags").createIndex({ category: 1 })

    this.database.collection("tagAliases").createIndex({ antecedentName: 1 })

    this.database.collection("posts").createIndex({ tags: 1 })
    this.database.collection("posts").createIndex({ flattenedTags: 1 })
    this.database.collection("posts").createIndex({ updatedAt: 1 })

    this.requester = new E621Requester(this)

    this.database.collection("posts").findOne({}).then(doc => {
      if (!doc) this.fetchAndApplyDatabaseExports()
      else this.updateAll()
    })

    this.singleton = this
  }

  async updateAll() {
    try {
      console.log("Beginning update")
      let t = Date.now()
      await this.requester.addNewPosts()
      console.log("New posts added")

      await this.requester.applyUpdates()
      console.log("Updates applied")

      await this.requester.getNewTagAliases()
      console.log("New tag aliases added")

      console.log(`Update complete. Took ${Date.now() - t}ms`)
    } catch (e) {
      console.error("Update failed")
      console.error(e)
    }

    setTimeout(() => { this.updateAll() }, 60000)
  }

  async fetchAndApplyDatabaseExports() {
    console.log("Starting export processing")
    let dateString = new Date().toISOString().split("T")[0]

    let startTime = Date.now()

    let postExport = await this.requester.getDatabaseExport(`posts-${dateString}.csv.gz`)
    let tagExport = await this.requester.getDatabaseExport(`tags-${dateString}.csv.gz`)
    let tagAliasExport = await this.requester.getDatabaseExport(`tag_aliases-${dateString}.csv.gz`)

    this.tagCache = {}

    let time = Date.now()
    await this.processTagExport(tagExport)
    tagExport.destroy()
    fs.rmSync(`tags-${dateString}.csv`)
    console.log(`Tag export processed in ${Date.now() - time}ms`)

    time = Date.now()
    await this.processPostExport(postExport)
    postExport.destroy()
    fs.rmSync(`posts-${dateString}.csv`)
    console.log(`Post export processed in ${Date.now() - time}ms`)

    time = Date.now()
    await this.processTagAliasExport(tagAliasExport)
    tagAliasExport.destroy()
    fs.rmSync(`tag_aliases-${dateString}.csv`)
    console.log(`Tag alias export processed in ${Date.now() - time}ms`)

    console.log(`Completed database parsing took: ${Date.now() - startTime}ms`)
    this.tagCache = null

    this.updateAll()
  }

  async processTagExport(stream) {
    let parser = stream.pipe(csv.parse({ columns: true, trim: true }))

    let update = async (tags) => {
      let now = Date.now()

      console.log("Batching 10000 tag updates")

      let bulk = []

      let cursor = this.database.collection("tags").find({ _id: { $in: tags.map(tag => tag.id) } })

      for await (let tag of cursor) {
        let newTag = tags.splice(tags.findIndex(t => t.id == tag._id), 1)[0]

        if (tag.category == newTag.category && tag.name == newTag.name) continue

        if (newTag) {
          bulk.push({
            updateOne: {
              filter: {
                _id: tag._id
              },
              update: { $set: { name: newTag.name, category: newTag.category } }
            }
          })
        }
      }

      for (let tag of tags) {
        bulk.push({
          insertOne: {
            document: { _id: tag.id, name: tag.name, category: tag.category }
          }
        })
      }

      if (bulk.length > 0) await this.database.collection("tags").bulkWrite(bulk)
      console.log(`Operation took ${Date.now() - now}ms`)
    }

    let tags = []

    for await (let data of parser) {
      if (tags.length > 10000) {
        await update(tags)

        tags.length = 0
      }

      data.id = parseInt(data.id)
      data.category = parseInt(data.category)
      data.name = data.name.toString()

      tags.push(data)
    }

    if (tags.length > 0) await update(tags)
  }

  async createPost(id, tags, uploaderId, approverId, createdAt, updatedAt, md5, sources, rating, width, height, duration,
    favoriteCount, score, parentId, children, fileType, fileSize, commentCount, isDeleted, isPending, isFlagged, isRatingLocked,
    isStatusLocked, isNoteLocked) {

    return new Promise((resolve) => {
      this.expandTagsToArray(tags).then(tags => {
        resolve({
          _id: id,
          tags: tags,
          flattenedTags: tags.flat(),
          uploaderId: isNaN(uploaderId) ? null : uploaderId,
          approverId: isNaN(approverId) ? null : approverId,
          createdAt: new Date(createdAt),
          updatedAt: new Date(updatedAt),
          md5: md5,
          sources: typeof (sources) == "string" ? sources.trim().split("\n").filter(s => s) : sources.filter(s => s),
          rating: rating,
          width: width,
          height: height,
          duration: isNaN(duration) ? 0 : duration,
          favoriteCount: favoriteCount,
          score: score,
          parentId: isNaN(parentId) ? null : parentId,
          children: children,
          fileType: fileType,
          fileSize: fileSize,
          commentCount: commentCount,
          isDeleted: isDeleted,
          isPending: isPending,
          isFlagged: isFlagged,
          isRatingLocked: isRatingLocked,
          isStatusLocked: isStatusLocked,
          isNoteLocked: isNoteLocked
        })
      })
    })
  }

  async expandTagsToArray(tags) {
    return new Promise(async (resolve) => {

      let toReturn = new Array(9).fill(null).map(() => [])

      if (typeof (tags) == "string") {
        for (let tagName of tags.split(" ")) {
          tagName = tagName.trim()

          let tag = await this.getTagByName(tagName)

          if (!tag) {
            tag = await this.getNewTag(tagName)

            if (!tag) {
              console.error(`Unable to get tag: ${tagName}`)
              continue
            }
          }

          toReturn[tag.category].push(tag._id)
        }
      } else {
        for (let tagNames of Object.values(tags)) {
          for (let tagName of tagNames) {
            tagName = tagName.trim()

            let tag = await this.getTagByName(tagName)

            if (!tag) {
              tag = await this.getNewTag(tagName)

              if (!tag) {
                console.error(`Unable to get tag: ${tagName}`)
                continue
              }
            }

            toReturn[tag.category].push(tag._id)
          }
        }
      }

      return resolve(toReturn)
    })
  }

  async processPostExport(stream) {
    let parser = stream.pipe(csv.parse({ columns: true, trim: true }))

    let update = async (posts) => {
      let now = Date.now()

      console.log("Batching 10000 post updates")

      let bulk = []

      let cursor = this.database.collection("posts").find({ _id: { $in: posts.map(post => post._id) } }, { _id: 1, updatedAt: 1 })

      for await (let post of cursor) {
        let newPost = posts.splice(posts.findIndex(p => p._id == post._id), 1)[0]

        if (newPost.updatedAt.getTime() == post.updatedAt.getTime()) continue

        if (newPost) {
          delete newPost._id
          bulk.push({
            updateOne: {
              filter: {
                _id: post._id,
              },
              update: { $set: newPost }
            }
          })
        }
      }

      for (let post of posts) {
        bulk.push({
          insertOne: {
            document: post
          }
        })
      }

      if (bulk.length > 0) await this.database.collection("posts").bulkWrite(bulk)
      console.log(`Operation took ${Date.now() - now}ms`)
    }

    let posts = []

    for await (let data of parser) {
      if (posts.length > 10000) {
        await update(await Promise.all(posts))

        posts.length = 0
      }

      if (!data.id) continue

      let p = this.createPost(parseInt(data.id), data.tag_string, parseInt(data.uploader_id), parseInt(data.approver_id), data.created_at + "Z", data.updated_at + "Z",
        data.md5, data.source, data.rating, parseInt(data.image_width), parseInt(data.image_height), parseFloat(data.duration), parseInt(data.fav_count),
        parseInt(data.score), parseInt(data.parent_id), [], data.file_ext, parseInt(data.file_size), parseInt(data.comment_count), data.is_deleted == "t",
        data.is_pending == "t", data.is_flagged == "t", data.is_rating_locked == "t", data.is_status_locked == "t", data.is_note_locked == "t")

      posts.push(p)
    }

    if (posts.length > 0) await update(posts)
  }

  async processTagAliasExport(stream) {
    let parser = stream.pipe(csv.parse({ columns: true, trim: true }))

    let update = async (tagAliases) => {
      let now = Date.now()

      console.log("Batching 10000 tag alias updates")

      let bulk = []

      let cursor = this.database.collection("tagAliases").find({ _id: { $in: tagAliases.map(alias => alias.id) } })
      let usedTags = await this.database.collection("tags").find({ name: { $in: tagAliases.map(alias => alias.consequent_name) } }, { _id: 1, name: 1 }).toArray()

      for await (let tagAlias of cursor) {
        let newAlias = tagAliases.splice(tagAliases.findIndex(t => t.id == tagAlias._id), 1)[0]

        if (newAlias) {
          if (newAlias.status == "active") {
            let usedTag = usedTags.find(tag => tag.name == tagAlias.consequent_name)
            if (usedTag) {
              newAlias.consequentId = usedTag._id
            } else {
              let tag = await this.getNewTag(tagAlias.consequent_name)

              if (!tag) {
                console.error(`Unable to get tag: ${tagAlias.consequent_name}`)
                continue
              }

              newAlias.consequentId = tag._id
              usedTags.push(tag)
            }

            if (tagAlias.antecedentName == newAlias.antecedent_name && tagAlias.consequentId == newAlias.consequentId) continue

            bulk.push({
              updateOne: {
                filter: {
                  _id: tagAlias._id
                },
                update: { $set: { antecedentName: newAlias.antecedent_name, consequentId: newAlias.consequentId } }
              }
            })
          } else {
            bulk.push({
              deleteOne: {
                filter: {
                  _id: newAlias.id
                }
              }
            })
          }
        }
      }

      for (let tagAlias of tagAliases) {
        if (tagAlias.status == "active") {
          let usedTag = usedTags.find(tag => tag.name == tagAlias.consequent_name)

          if (usedTag) {
            tagAlias.consequentId = usedTag._id
          } else {
            let tag = await this.getNewTag(tagAlias.consequent_name)

            if (!tag) {
              console.error(`Unable to get tag: ${tagAlias.consequent_name}`)
              continue
            }

            tagAlias.consequentId = tag._id
            usedTags.push(tag)
          }

          bulk.push({
            insertOne: {
              document: { _id: tagAlias.id, antecedentName: tagAlias.antecedent_name, consequentId: tagAlias.consequentId }
            }
          })
        }
      }

      if (bulk.length > 0) await this.database.collection("tagAliases").bulkWrite(bulk)
      console.log(`Operation took ${Date.now() - now}ms`)
    }

    let tagAliases = []

    for await (let data of parser) {
      if (tagAliases.length > 10000) {
        await update(tagAliases)

        tagAliases.length = 0
      }

      data.id = parseInt(data.id)
      data.antecedent_name = data.antecedent_name.toString()
      data.consequent_name = data.consequent_name.toString()

      tagAliases.push(data)
    }

    if (tagAliases.length > 0) await update(tagAliases)
  }

  async getLatestPostId() {
    return (await this.database.collection("posts").find({}, { _id: 1 }).sort({ _id: -1 }).limit(1).toArray())[0]._id
  }

  async getPost(id) {
    return await this.database.collection("posts").findOne({ _id: id })
  }

  async addPost(post) {
    if (!post._id) {
      console.error("Post with no id attempted to be added")
      return
    }
    await this.database.collection("posts").insertOne(post)
    await this.updateRelationships(post)
  }

  async updatePost(post) {
    if (!post._id) {
      console.error("Post with no id attempted to be replaced")
      return
    }
    await this.database.collection("posts").replaceOne({ _id: post._id }, post)
    await this.updateRelationships(post)
  }

  async updateRelationships(post) {
    if (!post.parentId) return
    await this.database.collection("posts").updateOne({ _id: post.parentId }, { $addToSet: { children: post._id } })
  }

  async getTags() {
    return await this.database.collection("tags").find({}).sort({ name: 1 }).toArray()
  }

  async getOrAddTag(tagName) {
    let tag = await this.getTagByName(tagName)
    if (tag) return tag

    tag = await this.getNewTag(tagName)
    if (tag) return tag

    return null
  }

  async getTag(id) {
    return await this.database.collection("tags").findOne({ _id: id })
  }

  async getTagByName(name) {
    if (this.tagCache && this.tagCache[name]) return this.tagCache[name]

    let tag = await this.database.collection("tags").findOne({ name })
    if (this.tagCache) this.tagCache[name] = tag

    return tag
  }

  async getNewTag(tagName) {
    let tag = await this.requester.getTag(tagName)

    if (tag) {
      await this.addTag(tag)
    }

    return tag
  }

  async addTag(tag) {
    await this.database.collection("tags").insertOne(tag)
  }

  async updateTag(tag) {
    await this.database.collection("tags").replaceOne({ _id: tag._id }, tag)
  }

  async getLatestTagAliasId() {
    return (await this.database.collection("tagAliases").find({}, { _id: 1 }).sort({ _id: -1 }).limit(1).toArray())[0]._id
  }

  async getTagAlias(id) {
    return await this.database.collection("tagAliases").findOne({ _id: id })
  }

  async getTagAliasByName(tagName) {
    return await this.database.collection("tagAliases").findOne({ antecedentName: tagName })
  }

  async addTagAlias(tagAlias) {
    return await this.database.collection("tagAliases").insertOne(tagAlias)
  }

  async updateTagAlias(tagAlias) {
    await this.database.collection("tagAliases").replaceOne({ _id: tagAlias._id }, tagAlias)
  }

  async deleteTagAlias(id) {
    return await this.database.collection("tagAliases").deleteOne({ _id: id })
  }

  getGroups(tags) {
    let tokenizer = new Tokenizer(tags)
    let currentGroupIndex = []
    let group = { tokens: [], groups: [] }

    for (let token of tokenizer) {
      let curGroup = group
      for (let group of currentGroupIndex) {
        curGroup = curGroup.groups[group]
      }

      if (token == "(") {
        currentGroupIndex.push(curGroup.groups.length)
        curGroup.groups.push({ tokens: [], groups: [] })
        curGroup.tokens.push(`__${curGroup.groups.length - 1}`)
      } else if (token == ")") {
        currentGroupIndex.splice(currentGroupIndex.length - 1, 1)
      } else {
        curGroup.tokens.push(token)
      }
    }

    if (currentGroupIndex.length != 0) {
      return [false, { status: 400, message: "Malformed tags, group not closed" }]
    }

    return [true, group]
  }

  async convertToTagIds(group) {
    for (let i = 0; i < group.tokens.length; i++) {
      let token = group.tokens[i]
      if (!TOKENS_TO_SKIP.includes(token) && !token.startsWith("__")) {
        if (token.includes("*")) {
          let regex = new RegExp("^" + token.replace("*", ".*") + "$")

          let tags = await this.database.collection("tags").find({ name: { $regex: regex } }).toArray()

          for (let j = 0; j < tags.length; j++) {
            group.tokens[i++] = tags[j]._id
            if (j < tags.length - 1) group.tokens[i++] = "~"
          }
        } else {
          let tag = await this.getOrAddTag(token)

          if (tag) {
            group.tokens[i] = tag._id
          } else {
            group.tokens[i] = ""
          }
        }
      }
    }

    group.tokens = group.tokens.filter(t => t != "")

    for (let g of group.groups) {
      await this.convertToTagIds(g)
    }
  }

  // ( a b c) ~ ( d e f ) means > (a & b & c) OR (d & e & f)
  //  a b ( c d ) means > a & b & c & d (nothing special)
  // -a b c means > not a, b & c
  // a b -( c d ) means > a & b, not (c & d) (meaning it can have either c or d, but not both)
  // a ~ b c means > (a OR b) & c
  // a ~ b ~ c d means > (a OR b OR c) & d
  // a ~ b ~ ( d e ) means > (a OR b OR (d and e)) (either a or b or the post has both d and e)
  // a ~ b ( -c ~ -e ) means > (a OR b & (not c OR not e)) (meaning a or b AND the post doesn't have c or the post doesn't have e)
  // ( -a ~ b ) means > (not a) OR b
  // a ^ b means > (a not b) OR (b not a) (exclusive or)
  // a b ^ c means > a and ((b not c) OR (c not b))
  // a ^ -b means > a and b OR not b and not a (due to the way exclusive or works, this sounds a little weird, but this is correct)
  // a ( b ( c ) ) means > a & b & c
  // a ( b ~ ( c e ) ) means > a & (b OR (c and e))
  // a ( b ^ ( c e ) ) means > a & ((b not (c & e)) OR (not b & (c & e))) | hopefully it's clear now that a ^ b is the same as ( a -b ) ~ ( -a b )

  async buildQueryFromGroup(group, curQuery = []) {
    let curExpression = {}

    let modifier = MODIFIERS.NONE

    for (let i = 0; i < group.tokens.length; i++) {
      let token = group.tokens[i]
      if (TOKENS_TO_SKIP.includes(token)) continue

      let previousToken = i > 0 ? group.tokens[i - 1] : null
      let nextToken = i < group.tokens.length - 1 ? group.tokens[i + 1] : null

      if (modifier == MODIFIERS.NONE) {
        
      }
    }

    return curQuery
  }

  async performSearch(tags, limit, page) {
    try {
      let [success, group] = this.getGroups(tags)

      if (!success) {
        return groups
      }

      await this.convertToTagIds(group)
      // let query = await this.buildQueryFromGroup(group)

      console.log(JSON.stringify(group, null, 4))
      // console.log(JSON.stringify(query, null, 4))

      return []
    } catch (e) {
      console.log(e)
      return { status: 500, message: "Internal server error" }
    }

  }
}

module.exports = Utilities