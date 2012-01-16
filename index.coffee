mongo = require "mongoskin"
express = require "express"

class ProfileResults
  @requestedAt: 0
  @operations: {}
  
  @getLatestResults: (callback) =>
    now = new Date()
    return callback(null, @operations) if @requestedAt > new Date(now.getTime() - 60 * 1000)
    
    range = 1000 * 60 * 60 * 24 * 7
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

# Endpoints
app.get "/slowqueries", (req, res) ->
  ProfileResults.getLatestResults (err, ops) ->
    res.render "operations",
      ops: ops
      
app.get "/stats", (req, res) ->
  ProfileResults.getLatestResults (err, ops) ->
    profileStats = {}
    for op in ops
      profileStats[op.collection] = {} unless profileStats[op.collection]?
      profileStats[op.collection].operations = {} unless profileStats[op.collection].operations?
      profileStats[op.collection].operations.total = 0 unless profileStats[op.collection].operations.total?
      profileStats[op.collection].operations[op.operation] = 0 unless profileStats[op.collection].operations[op.operation]?
      profileStats[op.collection].operations.total += 1
      profileStats[op.collection].operations[op.operation] += 1
    res.render "stats",
      profileStats: profileStats
    
console.log "Starting server on port 8080"