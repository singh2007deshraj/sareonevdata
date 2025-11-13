const Web3 = require("web3").default;
const express = require("express");
const cors = require("cors");
require("dotenv").config();
const app = express();
const mysql = require('mysql');
const http = require("http");
const cron = require("node-cron");
const { dexABI_MLM } = require("./config");
const conn=require('./connection');
app.use(express.json());
app.use(cors());
const web3 = new Web3(
  new Web3.providers.HttpProvider(process.env.RPCURL, {
    reconnect: { auto: true,delay: 5000, maxAttempts: 15, onTimeout: false,},
  })
);

const contract = new web3.eth.Contract(dexABI_MLM, process.env.CONTRACT_ADDRESS);

async function listEvent() {
  let lastSyncBlock = Number(await getLastSyncBlock());
  console.log(lastSyncBlock,"lastSyncBlock")
  let latestBlock = Number(await web3.eth.getBlockNumber());
    // console.log(latestBlock,"latestBlock")
  let toBlock =latestBlock > lastSyncBlock + 100 ? lastSyncBlock + 100 : latestBlock;
    console.log(toBlock,"toBlock")
  let events = await getEventReciept(lastSyncBlock, toBlock);
  await processEvents(events);
  await updateBlock(Number(toBlock));
  if (lastSyncBlock == toBlock) {
    setTimeout(listEvent, 15000);
  } else {
    setTimeout(listEvent, 5000);
  }
}
async function updateBlock(updatedBlock) {
  return new Promise((resolve, reject) => {
    conn.query(
      "UPDATE eventBlock SET latest_block = ?",[updatedBlock],(err, result) => {
        if (err) {
          console.error("Update Error:", err);
          return reject(err);
        }
        console.log("✅ Updated block:", updatedBlock);
        resolve(result);
      }
    );
  });
}
async function processEvents(events) {
  for (let i = 0; i < events.length; i++) {
    const { blockNumber, transactionHash, returnValues, event } = events[i];
     console.log(blockNumber, transactionHash, returnValues, event, "event");
    const timestamp = await getTimestamp(blockNumber);
    const newTimestamp = Number(timestamp) * 1000
   if (event === "Registration") {
       let referralIdn=await getuserIdnId(returnValues.referrer) || "0000";
      const checkSql = "SELECT id FROM Registration WHERE user = ?";
      conn.query(checkSql, [returnValues.user], (err, res) => {
        if (err) return console.error("DB Error:", err);
        if (res.length === 0) {
          const randomNumber = Math.floor(Math.random() * 10000000).toString().padStart(7, '0');
          const userRegId = randomNumber;
          const insertSql = `INSERT INTO Registration (userId, user_idn, user, referrer, referrerId,block_timestamp, transaction_id, block_number) VALUES (?, ?, ?, ?, ?, ?, ?,?)`;
          const values = [returnValues.userId,userRegId,returnValues.user,returnValues.referrer,referralIdn,newTimestamp,transactionHash,blockNumber,];
          conn.query(insertSql, values, (insertErr) => {
            if (insertErr) return console.error("Insert Error:", insertErr);
            console.log(`User registered: ${returnValues.user}`);
          });
        }
      });
    }
     else  if (event === "buyBoosting") {
      let userRegId=await getuserIdnId(returnValues.user) || "0000";
      const checkSql = "SELECT id FROM buyBoosting WHERE transaction_id = ?";
      conn.query(checkSql, [transactionHash], (err, res) => {
        if (err) return console.error("DB Error:", err);
        if (res.length === 0) {
          const insertSql = `INSERT INTO buyBoosting (user, user_idn, amount,levelsAdded, transaction_id, block_timestamp,block_number) VALUES (?, ?, ?, ?, ?, ?, ?)`;
          const values = [returnValues.user,userRegId,returnValues.amount,returnValues.levelsAdded,transactionHash,newTimestamp,blockNumber] ;
          conn.query(insertSql, values, (insertErr) => {
            if (insertErr) return console.error("Insert Error:", insertErr);
            console.log(`buy Boosting: ${transactionHash}`);
          });
        }
      });
    }
    else  if (event === "buyToken") {
      let userRegId=await getuserIdnId(returnValues.user) || "0000";
      const checkSql = "SELECT id FROM buyToken WHERE transaction_id = ?";
      conn.query(checkSql, [transactionHash], (err, res) => {
        if (err) return console.error("DB Error:", err);
        if (res.length === 0) {
          const insertSql = `INSERT INTO buyToken (user, user_idn, amount, transaction_id, block_timestamp,block_number, tokenQty, tokenRate) VALUES (?, ?, ?, ?, ?, ?, ?,?)`;
          const values = [returnValues.user,userRegId,returnValues.amount,transactionHash,newTimestamp,blockNumber,returnValues.tokenQty ,returnValues.tokenRate ];
          conn.query(insertSql, values, (insertErr) => {
            if (insertErr) return console.error("Insert Error:", insertErr);
            console.log(`token Buy: ${transactionHash}`);
          });
        }
      });
    }
    else  if (event === "Usertokenrecive") {
      let userRegId=await getuserIdnId(returnValues.user) || "0000";
      const insertSql = `INSERT INTO Usertokenrecive(user, user_idn,fromUser, transaction_id, block_timestamp,block_number, tokenQty, tokenRate,recType) VALUES (?, ?, ?, ?, ?, ?, ?,?,?)`;
          const values = [returnValues.user,userRegId,returnValues.fromUser,transactionHash,newTimestamp,blockNumber,returnValues.tokenQty ,returnValues.tokenRate,returnValues.recType ];
          conn.query(insertSql, values, (insertErr) => {
            if (insertErr) return console.error("Insert Error:", insertErr);
            console.log(`Event Usertokenrecive: ${transactionHash}`);
          });
    }
     else  if (event === "lapsLevelIncome") {
       let userRegId=await getuserIdnId(returnValues.user) || "0000";
      const insertSql = `INSERT INTO lapsLevelIncome (receiver, user_idn,sender,tokenQty,incomeAmt,incomeLevel,packageAmt, transaction_id, block_timestamp,block_number) VALUES (?, ?, ?, ?, ?, ?, ?,?,?,?)`;
          const values = [returnValues.receiver,userRegId,returnValues.sender,returnValues.tokenQty ,returnValues.incomeAmt,returnValues.incomeLevel,returnValues.packageAmt ,transactionHash,newTimestamp,blockNumber];
          conn.query(insertSql, values, (insertErr) => {
            if (insertErr) return console.error("Insert Error:", insertErr);
            console.log(`Event lapsLevelIncome : ${transactionHash}`);
          });
     }
     else  if (event === "userIncome") {
       let userRegId=await getuserIdnId(returnValues.user) || "0000";
      const insertSql = `INSERT INTO userIncome (receiver, user_idn,sender,tokenQty,incomeAmt,incomeLevel,packageAmt,incomeType, transaction_id, block_timestamp,block_number) VALUES (?, ?, ?, ?, ?, ?, ?,?,?,?)`;
          const values = [returnValues.receiver,userRegId,returnValues.sender,returnValues.tokenQty ,returnValues.incomeAmt,returnValues.incomeLevel,returnValues.packageAmt,returnValues.incomeType ,transactionHash,newTimestamp,blockNumber];
          conn.query(insertSql, values, (insertErr) => {
            if (insertErr) return console.error("Insert Error:", insertErr);
            console.log(`Event userIncome : ${transactionHash}`);
          });
     }
  }
}

async function getLastSyncBlock() {
  return new Promise((resolve, reject) => {
    conn.query("SELECT latest_block FROM eventBlock LIMIT 1", (err, result) => {
      if (err) return reject(err);

      if (!result || result.length === 0) {
        console.log("⚠️ No block found in eventBlock table");
        return resolve(0); // default start block
      }

      const fromBlock = Number(result[0].latest_block);
      if (isNaN(fromBlock)) {
        console.log("⚠️ Invalid block value in DB:", result[0].latest_block);
        return resolve(0);
      }

      resolve(fromBlock);
    });
  });
}

async function getuserIdnId(referrer) {
  return new Promise((resolve, reject) => {
    const sql = "SELECT user_idn FROM Registration WHERE user = ?";
    conn.query(sql, [referrer], (err, result) => {
      if (err) return reject(err);

      if (!result || result.length === 0) {
        return resolve("0000"); // default if no referral found
      }
      resolve(result[0].user_idn);
    });
  });
}


async function getEventReciept(fromBlock, toBlock) {
  let eventsData = await contract.getPastEvents("allEvents", {
    fromBlock: fromBlock,
    toBlock: toBlock,
  });
  return eventsData;
}
  async function getTimestamp(blockNumber) {
    let { timestamp } = await web3.eth.getBlock(blockNumber);
    return timestamp;
  }

function toFixed(x) {
  if (Math.abs(x) < 1.0) {
    var e = parseInt(x.toString().split("e-")[1]);
    if (e) {
      x *= Math.pow(10, e - 1);
      x = "0." + new Array(e).join("0") + x.toString().substring(2);
    }
  } else {
    var e = parseInt(x.toString().split("+")[1]);
    if (e > 20) {
      e -= 20;
      x /= Math.pow(10, e);
      x += new Array(e + 1).join("0");
    }
  }
  return String(x);
}

function round(number) {
  return Math.round(number * 1000) / 1000;
}


listEvent();
app.get("/", (req, res) => res.send("Server running!"));
app.listen(3000, () => console.log("Server on port 3000"));


 
