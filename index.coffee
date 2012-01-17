#!/usr/bin/env coffee

mongo = require "mongoskin"
express = require "express"
util = require "util"

class ProfileResults
  @requestedAt: {}
  @operations: {}
  
  @getKnownServers: () ->
    res = for key of @operations
      "#{key}"
    return res
    
  @getLatestResults: (server, db, callback) =>
    serverKey = server + "/" + db
    now = new Date()
    return callback(null, @operations[serverKey]) if @requestedAt[serverKey] > new Date(now.getTime() - 60 * 1000)
    
    range = 1000 * 60 * 60 * 24 * 1
    query =
      ts:
        $gte: new Date(now.getTime() - range)
        $lt: now

    mongo.db(serverKey).collection("system.profile").find(query, {sort: [["millis", -1]], slaveOk: true}).toArray (err, records) =>
      if (err)
        console.log "Error while grabbing profile #{err}"
        return callback(err, null)

      @requestedAt[serverKey] = now
      @operations[serverKey] = (ProfileResults.generateNormalizedOperation(r) for r in records)
      return callback(null, @operations[serverKey])

  @generateNormalizedQuery: (query) =>
    res = for key, value of query
      "#{key}"

    return res

  @generateNormalizedOperation: (profile) =>
    res =
      ts: profile.ts
      millis: profile.millis
      nscanned: profile.nscanned
      nreturned: profile.nreturned
      ntoreturn: profile.ntoreturn

    if profile.op == "command"
      if profile.command.count
        res.operation = "count"
        res.collection = profile.command.count
        res.query = profile.command.query
      else if profile.command.findandmodify
        res.operation = "findandmodify"
        res.collection = profile.command.findandmodify
        res.query = profile.command.update
      else
        res.operation = "unknown"
    else
      res.operation = profile.op
      res.collection = profile.ns
      res.payload = "TODO"
      res.query = profile.query
      res.query = profile.query.$query if profile.query && profile.query.$query
      res.query = profile.query.query if profile.query && profile.query.query

    res.normalized_query = ProfileResults.generateNormalizedQuery(res.query) if res.query

    return res
    
# Set up express
app = express.createServer();

app.configure ->
  app.set('views', __dirname + '/views');
  app.set('view engine', 'ejs');
  app.use(express.bodyParser());
  app.use(express.methodOverride());
  app.use(express.static(__dirname + '/public'));

app.configure "development", ->
  app.use(express.errorHandler({ dumpExceptions: true, showStack: true })); 

app.configure "production", ->
  app.use(express.errorHandler()); 

app.listen 8080

# App rendering helpers
app.helpers 
  queryFormatter: (query) ->
    ""

app.get "/", (req, res) ->
  res.render "serverList"
    servers: ProfileResults.getKnownServers()

app.get "/:server/:db", (req, res) ->
  res.render "index"
    server: req.params.server
    db: req.params.db

# Endpoints
app.get "/:server/:db/slowQueries", (req, res) ->
  ProfileResults.getLatestResults req.params.server, req.params.db, (err, ops) ->
    if err
      res.render "error",
        error: err
    else
      res.render "operations",
        ops: ops
      
app.get "/:server/:db/collectionStats", (req, res) ->
  ProfileResults.getLatestResults req.params.server, req.params.db, (err, ops) ->
    if err
      res.render "error",
        error: err
    else
      collectionStats = []
      collectionStatsLookup = {}
      for op in ops
        index = collectionStatsLookup[op.collection]
        if not index?
          index = collectionStats.push({collection: op.collection, operations: {totalOps: 0}}) - 1
          collectionStatsLookup[op.collection] = index
      
        collectionStats[index].operations.totalOps += 1
        collectionStats[index].operations[op.operation] = 0 unless collectionStats[index].operations[op.operation]?
        collectionStats[index].operations[op.operation] += 1
      
      collectionStats.sort (a,b) ->
        if a.operations.totalOps < b.operations.totalOps
          return 1
        else if a.operations.totalOps == b.operations.totalOps
          return 0
        else
          return -1
    
      res.render "collectionStats",
        collectionStats: collectionStats

app.get "/:server/:db/queryStats", (req, res) ->
  ProfileResults.getLatestResults req.params.server, req.params.db, (err, ops) ->
    if err
      res.render "error",
        error: err
    else
      queryStats = []
      queryStatsLookup = {}
      for op in ops
        if op.normalized_query?
          queryKey = if op.normalized_query.length == 0 then "NO_QUERY" else op.normalized_query
        else
          queryKey = "undefined"
        index = queryStatsLookup[op.operation + queryKey]
        if not index?
          index = queryStats.push({collection: op.collection, query: queryKey, operation: op.operation, totalExecutions: 0, totalMillis: 0, totalScanned: 0, totalReturned: 0 }) - 1
          queryStatsLookup[op.operation + queryKey] = index
      
        queryStats[index].totalExecutions += 1
        queryStats[index].totalMillis += if op.millis? then op.millis else 0
        queryStats[index].totalScanned += if op.nscanned? then op.nscanned else 0
        queryStats[index].totalReturned += if op.nreturned? then op.nreturned else 0
      
      queryStats.sort (a,b) ->
        if a.totalMillis < b.totalMillis
          return 1
        else if a.totalMillis == b.totalMillis
          return 0
        else
          return -1
      
      res.render "queryStats",
        queryStats: queryStats
    
console.log "Starting server on port 8080"