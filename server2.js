const express = require("express");
const bodyParser = require("body-parser")
const mariadb = require("mariadb")
const Redis = require('ioredis')

const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: true}))

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
        const sql = "SELECT * FROM Msg" //LIMIT 10
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

app.post("/api/msg", async (req,res) => {
    let body = req.body
    let maxScore
    let conn

    try {
        maxScore = await rc.zrange("index",-1,-1, "WITHSCORES")
        if (maxScore[1]) {
            maxScore = parseInt(maxScore[1]) + 1
        } else {
            maxScore = 0
        }

        await rc.zadd("index", maxScore, body.uuid)
        console.log("add data to cache update score "+ maxIndex)
        await rc.zadd("data", maxScore, JSON.stringify(body))
        console.log("add data to chache data score "+ maxIndex)
        
        res.status(201).json({
            success: true,
            message: "successfully post data",
        })

    } catch (err) {
        console.log(err)

    }

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

})

app.listen(3000, function() {
    console.log("Server started on port 3000");
});
  