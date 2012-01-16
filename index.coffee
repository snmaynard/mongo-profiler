mongo = require "mongoskin"
express = require "express"
util = require "util"

class ProfileResults
  @requestedAt: 0
  @operations: {}
  
  @getLatestResults: (callback) =>
    now = new Date()
    return callback(null, @operations) if @requestedAt > new Date(now.getTime() - 60 * 1000)
    
    range = 1000 * 60 * 60 * 24 * 1
    query =
      ts:
        $gte: new Date(now.getTime() - range)
        $lt: now

    db.collection("system.profile").find(query, {sort: [["millis", -1]]}).toArray (err, records) =>
      if (err)
        console.log "Error while grabbing profile #{err}"
        return callback(err, null)

      @requestedAt = now
      @operations = (ProfileResults.generateNormalizedOperation(r) for r in records)
      return callback(null, @operations)

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

# Set up db connection
# db = mongo.db("emongo2.heyzap.com/mobile")
db = mongo.db("localhost/mobile")

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
  res.render "index"

# Endpoints
app.get "/slowQueries", (req, res) ->
  ProfileResults.getLatestResults (err, ops) ->
    res.render "operations",
      ops: ops
      
app.get "/collectionStats", (req, res) ->
  ProfileResults.getLatestResults (err, ops) ->
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
      return a.operations.totalOps < b.operations.totalOps
    
    res.render "collectionStats",
      collectionStats: collectionStats

app.get "/queryStats", (req, res) ->
  ProfileResults.getLatestResults (err, ops) ->
    queryStats = []
    queryStatsLookup = {}
    for op in ops
      if op.normalized_query?
        queryKey = if op.normalized_query.length == 0 then "NO_QUERY" else op.normalized_query
      else
        queryKey = "undefined"
      index = queryStatsLookup[op.operation + queryKey]
      if not index?
        index = queryStats.push({collection: op.collection, query: queryKey, operation: op.operation, totalExecutions: 0, totalMillis: 0 }) - 1
        queryStatsLookup[op.operation + queryKey] = index
      
      
      queryStats[index].totalExecutions += 1
      queryStats[index].totalMillis += op.millis
      
    queryStats.sort (a,b) ->
      return a.totalMillis < b.totalMillis
      
    res.render "queryStats",
      queryStats: queryStats
    
console.log "Starting server on port 8080"