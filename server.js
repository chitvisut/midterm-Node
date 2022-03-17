const express = require("express");
const bodyParser = require("body-parser")
const mariadb = require("mariadb")

const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: true}))

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

app.get("/api/messages", function(req,res) {
    res.json({
        message: "Hello world!"
    })
});

// app.post("/api/messages", async (req, res) => {
//     let body = req.body;
//     console.log(body)
//     try {
//         const sql = "INSERT INTO Msg (uuid, author, message, likes) VALUES (?,?,?,?)"
//         const values = [body.uuid, body.author, body.message, body.likes]
//         const conn = await pool.getConnection();
//         const result = await conn.query(sql,values)
//         console.log(result)
//         res.status(200).json({
//             success: true,
//             message: "successfully insert row"
//         })
//     } catch (err) {
//         throw err;
//     }
// });

app.post("/api/msgs", async (req, res) => {
    let body = req.body;
    let conn
    try {
        const sql = "INSERT INTO Msg (uuid, author, message, likes) VALUES (?,?,?,?)"
        const values = [body.uuid, body.author, body.message, body.likes]
        conn = await pool.getConnection();
        const result = await conn.query(sql,values)
        console.log(result)
        res.status(200).json({
            success: true,
            message: "successfully insert row"
        })
    } catch (err) {
        throw err;

    } finally {
        if (conn) conn.release();
    }
});

app.listen(3000, function() {
    console.log("Server started on port 3000");
});
  