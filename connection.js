const mysql = require("mysql");

const conn = mysql.createPool({
  connectionLimit: 10, // handles multiple parallel queries safely
  host: "97.74.203.68",
  user: "sareon_MlmUser",
  password: "jkTqjT[P[mve",
  database: "sareon_Mlmdb",
});

console.log("✅ MySQL Pool Created");

module.exports = conn;
