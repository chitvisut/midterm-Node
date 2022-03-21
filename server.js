const express = require("express");
const bodyParser = require("body-parser")
const mariadb = require("mariadb")
const Redis = require('ioredis')

const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: true}))

// const rc = Redis.createClient({
//     url: 'redis://localhost:6379'
// })
const rc = new Redis()

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

//to be changed
app.get("/api/messages", async (req,res) => {
    let conn
    try {
        //const sql = "SELECT * FROM Msg OFFSET 5 ROWS FETCH NEXT 5 ROWS ONLY"
        const sql = "SELECT * FROM Msg" //LIMIT 10"
        conn = await pool.getConnection();

        const result = await conn.query(sql)
        console.log(result)

        res.status(201).json({
            success: true,
            message: "successfully get all rows",
            data: result
        })
    } catch (err) {
        throw err;

    } finally {
        if (conn) conn.release();
    }
});

app.get("/api/msg", async (req,res) => {
    let body = req.body
    let rCount = parseInt(body.count)
    let count = await rc.zrange("count", -1, -1, "WITHSCORES")
    count = parseInt(count[1])

    //case nothong to update
    if (count === rCount) {
        res.status(201).json({
                success: true,
                message: "client is up to date"
            })
        }

    let minCount = await rc.zrevrange("data", -1, -1, "WITHSCORES")
    minCount = parseInt(minCount[1])

    let cacheResult
    let result
    console.log(rCount)
    console.log(minCount)

    //case all update need is in cache
    if (minCount <= rCount + 1) {
        cacheResult = await rc.zrangebyscore("data", rCount + 1, count)
        result = []
        cacheResult.forEach((string) => result.push(JSON.parse(string)))
        cacheResult = undefined 
        res.status(201).json({
            success: true,
            data: result
        })
    } else { //case update from both cache and DB
        cacheResult = await rc.zrangebyscore("data", rCount + 1, count)
        console.log(cacheResult)
        result = []
        //cacheResult.forEach((string) => result.push(JSON.parse(string)))
        cacheResult.forEach((string) => result.push(string))
        cacheResult = undefined

        let conn
            try {
                let sql = "SELECT uuid author message likes FROM Data WHERE count >= " + (rCount+1) + " AND " + "count < " + count + " AND isdelete = 0"
                console.log(sql)
                conn = await pool.getConnection();

                const result = await conn.query(sql)
                console.log(result)

                res.status(201).json({
                    success: true,
                    message: "successfully get all rows",
                    //data: result
                })
            } catch (err) {
                throw err;

            } finally {
                if (conn) conn.release();
            }

        // res.status(201).json({
        //     success: true,
        //     data: result
        // })
    }


    //res.status(201).json({success: true})

    // let conn
    // try {
    //     //const sql = "SELECT * FROM Msg OFFSET 5 ROWS FETCH NEXT 5 ROWS ONLY"
    //     const sql = "SELECT * FROM Msg" //LIMIT 10
    //     conn = await pool.getConnection();

    //     const result = await conn.query(sql)
    //     console.log(result)

    //     res.status(201).json({
    //         success: true,
    //         message: "successfully get all rows",
    //         data: result
    //     })
    // } catch (err) {
    //     throw err;

    // } finally {
    //     if (conn) conn.release();
    // }
});

app.get("/api/messages/:uuid", async (req,res) => {
    let conn
    try {
        const sql = "SELECT * FROM Msg WHERE uuid = ?"
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
    try {
        let sql = "SELECT uuid FROM Msg WHERE uuid = ?"
        let values = [req.params.uuid]
        conn = await pool.getConnection();

        let [rows, meta] = await conn.query(sql,values)

        if (!rows) {
            res.status(404).json({
                success: false,
                message: "uuid not found",
            })
        } else {
            sql = "UPDATE Msg SET author = ?, message = ?, likes = ? WHERE uuid = ?"
            values = [req.body.author, req.body.message, req.body.likes, req.params.uuid]
            let result = await conn.query(sql,values)
            res.status(201).json({
                success: true,
                message: "successfully update",
                //data: result
            })
        }

    } catch (err) {
        throw err;

    } finally {
        if (conn) conn.release();
    }
});

//delete version
app.delete("/api/messages/:uuid", async (req,res) => {
    let conn
    try {
        let sql = "SELECT uuid FROM Msg WHERE uuid = ?"
        let values = [req.params.uuid]
        conn = await pool.getConnection();

        let [rows, meta] = await conn.query(sql,values)

        if (!rows) {
            res.status(404).json({
                success: false,
                message: "uuid not found",
            })
        } else {
            sql = "DELETE FROM Msg WHERE uuid = ?"
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
    try {
        let sql = "SELECT uuid FROM Msg WHERE uuid = ?"
        let values = [body.uuid]
        conn = await pool.getConnection();
        let [rows, meta] = await conn.query(sql,values)

        if (rows) {
            res.status(409).json({
                success: false,
                message: "uuid already exist",
            })
        } else {
            sql = "INSERT INTO Msg (uuid, author, message, likes) VALUES (?,?,?,?)"
            values = [body.uuid, body.author, body.message, body.likes]
            conn = await pool.getConnection();
            const result = await conn.query(sql,values)
            //console.log(result)
            res.status(201).json({
                success: true,
                message: "successfully insert row"
            })
        }

    } catch (err) {
        throw err;

    } finally {
        if (conn) conn.release();
    }
});

app.post("/api/msg/:uuid", async (req,res) => {
    let body = req.body
    let maxIndex

    body.uuid = req.params.uuid
    console.log(body)

    try {
    //await rc.connect()
    //maxIndex = await rc.zRangeWithScores("index",-1,-1)
    // maxIndex = await rc.zrange("index",-1,-1, "WITHSCORES")

    // console.log(maxIndex)
    // if (maxIndex[1]) {
    //     maxIndex = parseInt(maxIndex[1]) + 1
    // } else {
    //     maxIndex = 0
    // }

    // await rc.zAdd("index", { score: maxIndex, value: body.uuid})
    // console.log("add data to cache update score "+ maxIndex)
    // await rc.zAdd("data", { score: maxIndex, value: JSON.stringify(body)})
    // console.log("add data to chache data score "+ maxIndex)

    // await rc.zadd("index", maxIndex, body.uuid)
    // console.log("add data to cache update score "+ maxIndex)
    // await rc.zadd("data", maxIndex, JSON.stringify(body))
    // console.log("add data to chache data score "+ maxIndex)

    await rc.zadd("count", 1, "A")
    await rc.zadd("data", 1, "A1")

    await rc.zadd("count", 2, "B")
    await rc.zadd("data", 2, "B1")

    await rc.zadd("count", 3, "C")
    await rc.zadd("data", 3, "C1")

    await rc.zadd("count", 4, "D")
    await rc.zadd("data", 4, "D1")

    await rc.zadd("count", 5, "A")
    await rc.zremrangebyscore("data", 1, 1)
    await rc.zadd("data", 5, "A2")

    await rc.zadd("count", 6, "B")
    await rc.zremrangebyscore("data", 2, 2)
    await rc.zadd("data", 6, "B2")

    await rc.zrem("count", "D")
    await rc.zremrangebyscore("data", 4, 4)

    // console.log("start query")
    // const rkeys = await redisClient.keys('*')
    // let test
    // rkeys.forEach( async (key) => {
    //     test = await redisClient.get(key)
    //     if (test > 999900) {
    //         console.log(key)
    //     }
    // })
    // redisClient.keys('*', function(err, results) {
    //     results.forEach(function(key) {
    //       redis.hget(key, 'statut', function(err, statut) {
    //         if (parseInt(statut) === 2) {
    //           console.log(key, statut);
    //         }
    //       });
    //     });
    //   });
    //await redisClient.zAdd("index", 1, "A");
    // await redisClient.zAdd("index", 2, "B");
    // await redisClient.zAdd("index", 3, "C");
    // await redisClient.zAdd("index", 4, "D");

    //const value = await redisClient.get(body.uuid)

    // console.log("start query")
    //const value = await rc.zcount("index", "-inf", "+inf")
    //const value = await rc.zrevrange("count", -1, -1, "WITHSCORES")
    const validid = await rc.zrangebyscore("count", 0, 3)
    const oscore = await rc.zscore("data", "B")
    const xcount = await rc.zrange("count", 0, -1, "WITHSCORES")
    const xdata = await rc.zrange("data", 0, -1, "WITHSCORES")
    // const result = []
    // value.forEach((string) => result.push(JSON.parse(string)))

    //console.log(parseInt(value[1]))
    if (oscore) {
        console.log(oscore)
    } else {
        console.log("not found")
    }

    console.log(validid)
    console.log(xcount)
    console.log(xdata)

    res.status(201).json({
        success: true,
        message: "successfully get all rows",
        data: [body]
    })

} catch (err) {
    console.log(err)

} 
// finally {
//     rc.quit()
// }

})

app.listen(3000, function() {
    console.log("Server started on port 3000");
});
  