const express = require("express");
const bodyParser = require("body-parser")
const mariadb = require("mariadb")
const Redis = require('ioredis')

const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: true}))

const rc = new Redis()
const maxCache = 200000

const pool = mariadb.createPool({
    host: "34.217.133.174", 
    port: 3306,
    user: "user1", 
    password: "password1",
    database: "midterm"
});

async function checkDbConnection() {
    let conn;
    try {
      conn = await pool.getConnection();
      const rows = await conn.query("SHOW TABLES");
      console.log("successfully connected to MariaDB")
      console.log(rows);
    } catch (err) {
      console.log(err)
        throw err;

    } finally {
        if (conn) conn.release();
    }
  }

checkDbConnection()

app.get("/api/messages", async (req,res) => {
    let body = req.body
    let rCount = parseInt(body.count)
    let count = await rc.zrange("count", -1, -1, "WITHSCORES")
    count = parseInt(count[1])

    //case nothong to update
    if (count === rCount) {
        res.status(201).json({
                success: true,
                message: "client is up to date",
                count: count
            })
    } else {

        let minCount = await rc.zrevrange("data", -1, -1, "WITHSCORES")
        minCount = parseInt(minCount[1])

        let validId = await rc.zrangebyscore("count", 0, rCount)

        let cacheResult
        let cresult
        let result
        console.log(rCount)
        console.log(minCount)

        //case all update need is in cache
        if (minCount <= rCount + 1) {
            cacheResult = await rc.zrangebyscore("data", rCount + 1, count)
            cresult = []
            cacheResult.forEach((string) => cresult.push(JSON.parse(string)))
            cacheResult = undefined 
            res.status(201).json({
                success: true,
                message: "successfully get all rows",
                count: count,
                valid: validId,
                data: cresult
            })
        } else { //case update from both cache and DB
            cacheResult = await rc.zrangebyscore("data", minCount, count)
            console.log(cacheResult)
            cresult = []
            cacheResult.forEach((string) => cresult.push(JSON.parse(string)))
            cacheResult = undefined


            let conn
                try {
                    let sql = "SELECT uuid, author, message, likes FROM data WHERE count >= " + (rCount+1) + " AND " + "count < " + (minCount) + " AND isdelete = 0"
                    console.log(sql)
                    conn = await pool.getConnection();

                    result = await conn.query(sql)
                    cresult.forEach((item)=>result.push(item))
                    cresult = undefined

                    console.log("prepare to send res")
                    res.status(201).json({
                        success: true,
                        message: "successfully get all rows",
                        count: count,
                        valid: validId,
                        data: result,
                    })

                } catch (err) {
                    throw err;

                } finally {
                    if (conn) conn.release();
                }
        }
    }
});

app.get("/api/messages/:uuid", async (req,res) => {
    let conn
    try {
        const sql = "SELECT * FROM data WHERE uuid = ? AND isdelete = 0"
        const values = [req.params.uuid]
        conn = await pool.getConnection();

        const [row, meta] = await conn.query(sql, values)

        if (!row) {
            res.status(404).json({
                success: false,
                message: "uuid not found"
            })
        } else {
            res.status(201).json({
                success: true,
                message: "successfully get a row",
                data: row
            })
        }

    } catch (err) {
        throw err;

    } finally {
        if (conn) conn.release();
    }
});

app.put("/api/messages/:uuid", async (req,res) => {
    let conn
    let body = req.body
    body.uuid = req.params.uuid

    //check count number in cache
    let count = await rc.zrange("count", -1, -1, "WITHSCORES")
    count = parseInt(count[1])
    console.log(count)

    //find score of uuid
    let oscore = await rc.zscore("count", req.params.uuid)

    if (oscore) {
        //update count and data table in cache
        await rc.zadd("count", count + 1, req.params.uuid)
        await rc.zremrangebyscore("data", oscore, oscore)
        await rc.zadd("data", count + 1, JSON.stringify(body))

        //DB part
        try {
            conn = await pool.getConnection();
            sql = "UPDATE data SET author = ?, message = ?, likes = ? , count = ? WHERE uuid = ?"
            values = [req.body.author, req.body.message, req.body.likes, count + 1, req.params.uuid]
            let result = await conn.query(sql,values)
            res.status(201).json({
                success: true,
                message: "successfully update",
            })
    

        } catch (err) {
            throw err;

        } finally {
            if (conn) conn.release();
        }

    } else {
        res.status(404).json({
            success: false,
            message: "uuid not found in count cache",
    })}
});

//delete version
app.delete("/api/messages/:uuid", async (req,res) => {

    let conn
    try {
        let sql = "SELECT uuid FROM data WHERE uuid = ? and isdelete = 0"
        let values = [req.params.uuid]
        conn = await pool.getConnection();

        let [rows, meta] = await conn.query(sql,values)

        if (!rows) {
            res.status(404).json({
                success: false,
                message: "uuid not found in DB",
            })
        } else {
            //Update count Cache
            let count = await rc.zrange("count", -1, -1, "WITHSCORES")
            count = parseInt(count[1])
            console.log(count)
            await rc.zadd("count", count + 1, req.params.uuid)
            //Update data Cache
            //find score of uuid
            let oscore = await rc.zscore("count", req.params.uuid)
            let cache = await rc.zrangebyscore("data", oscore, oscore)
            if (cache) {
                await rc.zremrangebyscore("data", oscore, oscore)
            }

            //uodate isdelete instead of delete
            sql = "UPDATE data SET isdelete = 1 WHERE uuid = ?"
            values = [req.params.uuid]
            let result = await conn.query(sql,values)
            res.status(201).json({
                success: true,
                message: "successfully delete",
                //data: result
            })
        }

    } catch (err) {
        throw err;

    } finally {
        if (conn) conn.release();
    }
})

app.post("/api/messages", async (req, res) => {
    let body = req.body;
    let conn

    console.log(body)
    //check count number in cache
    let count = await rc.zrange("count", -1, -1, "WITHSCORES")
    count = parseInt(count[1]) || 0
    console.log("count " + count)

    //find score of uuid
    let oscore = await rc.zscore("count", body.uuid)
    console.log("oscore " + oscore)


    if (!oscore) {
        let cacheSize = await rc.zcount("data", "-inf", "+inf")
        if (cacheSize < maxCache) {
            //update count and data table in cache
            await rc.zadd("count", count + 1, body.uuid)
            await rc.zadd("data", count + 1, JSON.stringify(body)) 
        } else {
            //remove oldest cache and then update count and data table in cache
            let leastScore = await rc.zrevrange("data", -1, -1, "WITHSCORES")
            leastScore = parseInt(leastScore[1])
            await rc.zremrangebyscore("data", leastScore, leastScore)
            await rc.zadd("count", count + 1, body.uuid)
            await rc.zadd("data", count + 1, JSON.stringify(body))
        }

        try {

            conn = await pool.getConnection();
            sql = "INSERT INTO data (uuid, author, message, likes, count) VALUES (?,?,?,?,?)"
            values = [body.uuid, body.author, body.message, body.likes, count + 1]
            const result = await conn.query(sql,values)
            res.status(201).json({
                success: true,
                message: "successfully insert row"
            })
    
        } catch (err) {
            throw err;
    
        } finally {
            if (conn) conn.release();
        }

    } else {
        res.status(409).json({
            success: false,
            message: "uuid already exist in cache",
    })}

});

app.listen(3000, function() {
    console.log("Server started on port 3000");
});
  