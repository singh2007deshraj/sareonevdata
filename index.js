// final_safe_events_processor.js
const Web3 = require("web3").default;
const express = require("express");
const cors = require("cors");
require("dotenv").config();
const app = express();
const mysql = require('mysql');
const http = require("http");
const cron = require("node-cron");
const { dexABI_MLM } = require("./config");
const conn = require('./connection');
app.use(express.json());
app.use(cors());
const web3 = new Web3(
  new Web3.providers.HttpProvider(process.env.RPCURL, {
    reconnect: { auto: true, delay: 5000, maxAttempts: 15, onTimeout: false, },
  })
);

const contract = new web3.eth.Contract(dexABI_MLM, process.env.CONTRACT_ADDRESS);

/* --------------------------
   Helper: Promise wrapper for conn.query
   -------------------------- */
function runQuery(sql, params = []) {
  return new Promise((resolve, reject) => {
    conn.query(sql, params, (err, result) => {
      if (err) return reject(err);
      resolve(result);
    });
  });
}

/* --------------------------
   Safe helper to fetch user_idn (falls back to "0000")
   -------------------------- */
async function safeUserIdn(addr) {
  try {
    if (!addr) return "0000";
    const idn = await getuserIdnId(addr);
    return idn || "0000";
  } catch (e) {
    console.error("safeUserIdn error:", e, addr);
    return "0000";
  }
}

/* --------------------------
   Main event-listing & scheduling
   -------------------------- */
async function listEvent() {
  try {
    let lastSyncBlock = Number(await getLastSyncBlock());
    let latestBlock = Number(await web3.eth.getBlockNumber());
    console.log("lastSyncBlock : ", lastSyncBlock);
    console.log("latestBlock : ", latestBlock);
    // Always move forward by at least 1 block
    let fromBlock = lastSyncBlock + 1;
    if (fromBlock > latestBlock) {
      // No new blocks → wait and retry
      console.log("No new blocks...");
      return setTimeout(listEvent, 10000);
    }
    // Limit batch size (100)
    let toBlock = fromBlock + 2000;
    if (toBlock > latestBlock) toBlock = latestBlock;
    console.log(new Date());
    console.log("New block");
    console.log({ fromBlock, toBlock });
    let events = await getEventReciept(fromBlock, toBlock);
    if (events && events.length > 0) {
      await processEvents(events);
    } else {
      console.log("No events in range");
    }
    await updateBlock(toBlock);
  } catch (err) {
    console.error("listEvent error:", err);
  }
}

/* Cron: run at minute 1 of every hour (keeps running) */
cron.schedule("* * * * *", async () => {
  try {
    await listEvent();
    console.log("Cron job executed successfully");
  } catch (err) {
    console.error("Cron job error:", err);
  }
});

// initial run at startup
listEvent();

/* --------------------------
   Update last processed block
   -------------------------- */
async function updateBlock(updatedBlock) {
  return runQuery("UPDATE eventBlock SET latest_block = ?", [updatedBlock])
    .then((res) => {
      console.log("✅ Updated block:", updatedBlock);
      return res;
    })
    .catch((err) => {
      console.error("Update Error:", err);
      throw err;
    });
}

/* --------------------------
   NEW: safe processEvents + dispatcher
   -------------------------- */
async function processEvents(events) {
  for (let i = 0; i < events.length; i++) {
    const ev = events[i];
    try {
      await handleEvent(ev);
    } catch (err) {
      console.error("Error processing event (continuing):", err, {
        index: i,
        transactionHash: ev && ev.transactionHash,
        event: ev && ev.event,
      });
      // continue to next event
    }
  }
}

async function handleEvent(eventObj) {
  const { blockNumber, transactionHash, returnValues, event } = eventObj;
  console.log(blockNumber, transactionHash, returnValues, event, "event");
  const timestamp = await getTimestamp(blockNumber);
  const ts = timestamp;

  switch (event) {
    case "Registration":
      return handle_Registration(returnValues, transactionHash, ts, blockNumber);

    case "buyBoosting":
      return handle_buyBoosting(returnValues, transactionHash, ts, blockNumber);

    case "buyToken":
      return handle_buyToken(returnValues, transactionHash, ts, blockNumber);

    case "saleTokenE":
      return handle_saleTokenE(returnValues, transactionHash, ts, blockNumber);

    case "Usertokenrecive":
      return handle_Usertokenrecive(returnValues, transactionHash, ts, blockNumber);

    case "lapsLevelIncome":
      return handle_lapsLevelIncome(returnValues, transactionHash, ts, blockNumber);

    case "userIncome":
      return handle_userIncome(returnValues, transactionHash, ts, blockNumber);

    case "withdrawal":
      return handle_withdrawal(returnValues, transactionHash, ts, blockNumber);

    case "magicBooster":
      return handle_magicBooster(returnValues, transactionHash, ts, blockNumber);

    case "magicBoosterUnstack":
      return handle_magicBoosterUnstack(returnValues, transactionHash, ts, blockNumber);

    case "claimMBoosterIncome":
      return handle_claimMBoosterIncome(returnValues, transactionHash, ts, blockNumber);

    default:
      console.log("Unknown event:", event);
      return;
  }
}

/* --------------------------
   Individual handlers (all promise-based)
   -------------------------- */

async function handle_Registration(rv, tx, ts, block) {
  try {
    const referralIdn = await safeUserIdn(rv.referrer);
    const exists = await runQuery("SELECT id FROM Registration WHERE user = ?", [rv.user]);

    if (exists && exists.length > 0) return; // already exists

    const userRegId = Math.floor(Math.random() * 10000000).toString().padStart(7, "0");
    const insertSql = `INSERT INTO Registration (userId, user_idn, user, referrer, referrerId, block_timestamp, transaction_id, block_number) VALUES (?, ?, ?, ?, ?, ?, ?,?)`;
    const values = [rv.userId, userRegId, rv.user, rv.referrer, referralIdn, ts, tx, block];

    await runQuery(insertSql, values);
    console.log(`User registered: ${rv.user}`);
  } catch (err) {
    console.error("Registration handler error:", err, { rv, tx });
  }
}

async function handle_buyBoosting(rv, tx, ts, block) {
  try {
    const userRegId = await safeUserIdn(rv.user);
    const checkSql = "SELECT id FROM buyBoosting WHERE transaction_id = ?";
    const found = await runQuery(checkSql, [tx]);
    if (found && found.length > 0) return;

    const insertSql = `INSERT INTO buyBoosting (user, user_idn, amount, levelsAdded, transaction_id, block_timestamp, block_number) VALUES (?, ?, ?, ?, ?, ?, ?)`;
    const values = [rv.user, userRegId, rv.amount, rv.levelsAdded, tx, ts, block];
    await runQuery(insertSql, values);
    console.log(`buy Boosting saved: ${tx}`);
  } catch (err) {
    console.error("buyBoosting handler error:", err, { rv, tx });
  }
}

async function handle_buyToken(rv, tx, ts, block) {
  try {
    const userRegId = await safeUserIdn(rv.user);
    const values = [rv.user, userRegId, rv.amount, tx, ts, block, rv.tokenQty, rv.tokenRate];
    const checkSql = "SELECT id FROM buyToken WHERE user=? AND user_idn=? AND amount=? AND transaction_id=? AND block_timestamp=? AND block_number=? AND tokenQty=? AND tokenRate=?";
    const found = await runQuery(checkSql, values);
    if (found && found.length > 0) return;

    const insertSql = `INSERT INTO buyToken (user, user_idn, amount, transaction_id, block_timestamp, block_number, tokenQty, tokenRate) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;
    await runQuery(insertSql, values);
    console.log(`buyToken saved: ${tx}`);
  } catch (err) {
    console.error("buyToken handler error:", err, { rv, tx });
  }
}

async function handle_saleTokenE(rv, tx, ts, block) {
  try {
    const userRegId = await safeUserIdn(rv.user);
    const values = [rv.user, userRegId, rv.amount, rv.adminAmt, tx, ts, block, rv.tokenQty, rv.tokenRate];
    const checkSql = "SELECT id FROM saleTokenE WHERE user=? AND user_idn=? AND amount=? AND adminAmt=? AND transaction_id=? AND block_timestamp=? AND block_number=? AND tokenQty=? AND tokenRate=?";
    const found = await runQuery(checkSql, values);
    if (found && found.length > 0) return;

    const insertSql = `INSERT INTO saleTokenE (user, user_idn, amount, adminAmt, transaction_id, block_timestamp, block_number, tokenQty, tokenRate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    await runQuery(insertSql, values);
    console.log(`saleTokenE saved: ${tx}`);
  } catch (err) {
    console.error("saleTokenE handler error:", err, { rv, tx });
  }
}

async function handle_Usertokenrecive(rv, tx, ts, block) {
  try {
    const userRegId = await safeUserIdn(rv.user);
    const values = [rv.user, userRegId, rv.fromUser, tx, ts, block, rv.tokenQty, rv.tokenRate, rv.recType];
    const checkSql = "SELECT id FROM Usertokenrecive WHERE user=? AND user_idn=? AND fromUser=? AND transaction_id=? AND block_timestamp=? AND block_number=? AND tokenQty=? AND tokenRate=? AND recType=?";
    const found = await runQuery(checkSql, values);
    if (found && found.length > 0) return;

    const insertSql = `INSERT INTO Usertokenrecive(user, user_idn, fromUser, transaction_id, block_timestamp, block_number, tokenQty, tokenRate, recType) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    await runQuery(insertSql, values);
    console.log(`Usertokenrecive saved: ${tx}`);
  } catch (err) {
    console.error("Usertokenrecive handler error:", err, { rv, tx });
  }
}

async function handle_lapsLevelIncome(rv, tx, ts, block) {
  try {
    const idnTarget = rv.receiver || rv.user || rv.receiverAddress;
    const userRegId = await safeUserIdn(idnTarget);
    const values = [rv.receiver, userRegId, rv.sender, rv.tokenQty, rv.incomeAmt, rv.incomeLevel, rv.packageAmt, tx, ts, block];
    const checkStr = `SELECT id FROM lapsLevelIncome WHERE receiver = ? AND user_idn = ? AND sender = ? AND tokenQty = ? AND incomeAmt = ? AND incomeLevel = ? AND packageAmt = ? AND transaction_id = ? AND block_timestamp = ? AND block_number = ?`;
    const found = await runQuery(checkStr, values);
    if (found && found.length > 0) return;

    const insertSql = `INSERT INTO lapsLevelIncome (receiver, user_idn, sender, tokenQty, incomeAmt, incomeLevel, packageAmt, transaction_id, block_timestamp, block_number) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    await runQuery(insertSql, values);
    console.log(`lapsLevelIncome saved: ${tx}`);
  } catch (err) {
    console.error("lapsLevelIncome handler error:", err, { rv, tx });
  }
}

async function handle_userIncome(rv, tx, ts, block) {
  try {
    const idnTarget = rv.receiver || rv.user || rv.to;
    const userRegId = await safeUserIdn(idnTarget);
    const tokenQty = rv.tokenQty || 0;
    const values = [rv.receiver, userRegId, rv.sender, tokenQty, rv.incomeAmt, rv.incomeLevel, rv.packageAmt, rv.incomeType, tx, ts, block];
    const checkSql = `SELECT id FROM userIncome WHERE receiver=? AND user_idn=? AND sender=? AND tokenQty=? AND incomeAmt=? AND incomeLevel=? AND packageAmt=? AND incomeType=? AND transaction_id=? AND block_timestamp=? AND block_number=?`;
    const found = await runQuery(checkSql, values);
    if (found && found.length > 0) return;

    const insertSql = `INSERT INTO userIncome (receiver, user_idn, sender, tokenQty, incomeAmt, incomeLevel, packageAmt, incomeType, transaction_id, block_timestamp, block_number) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    await runQuery(insertSql, values);
    console.log(`userIncome saved: ${tx}`);
  } catch (err) {
    console.error("userIncome handler error:", err, { rv, tx });
  }
}

async function handle_withdrawal(rv, tx, ts, block) {
  try {
    const userRegId = await safeUserIdn(rv.user);
    const insertSql = `INSERT INTO withdrawal (user, user_idn, usdtAmount, adminAmount, transaction_id, block_timestamp, block_number) VALUES (?, ?, ?, ?, ?, ?, ?)`;
    const values = [rv.user, userRegId, rv.usdtAmount, rv.adminAmount, tx, ts, block];
    await runQuery(insertSql, values);
    console.log(`withdrawal saved: ${tx}`);
  } catch (err) {
    console.error("withdrawal handler error:", err, { rv, tx });
  }
}

async function handle_magicBooster(rv, tx, ts, block) {
  try {
    const userRegId = await safeUserIdn(rv.user);
    const values = [rv.lastStakeId, rv.user, userRegId, rv.stackamt, rv.unstackAmt, rv.startdate, rv.enddate, rv.status, tx, ts, block];
    const checkSql = `SELECT id FROM magicBooster WHERE lastStakeId=? AND user=? AND user_idn=? AND stackamt=? AND unstackAmt=? AND startdate=? AND enddate=? AND status=? AND transaction_id=? AND block_timestamp=? AND block_number=?`;
    const found = await runQuery(checkSql, values);
    if (found && found.length > 0) return;

    const insertSql = `INSERT INTO magicBooster (lastStakeId, user, user_idn, stackamt, unstackAmt, startdate, enddate, status, transaction_id, block_timestamp, block_number) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    await runQuery(insertSql, values);
    console.log(`magicBooster saved: ${tx}`);
  } catch (err) {
    console.error("magicBooster handler error:", err, { rv, tx });
  }
}

async function handle_magicBoosterUnstack(rv, tx, ts, block) {
  try {
    const userRegId = await safeUserIdn(rv.user);
    const values = [rv.lastStakeId, rv.user, userRegId, rv.stackamt, rv.unstackAmt, rv.unstackDate, tx, ts, block];
    const checkSql = `SELECT id FROM magicBoosterUnstack WHERE lastStakeId=? AND user=? AND user_idn=? AND stackamt=? AND unstackAmt=? AND unstackDate=? AND transaction_id=? AND block_timestamp=? AND block_number=?`;
    const found = await runQuery(checkSql, values);
    if (found && found.length > 0) return;

    const insertSql = `INSERT INTO magicBoosterUnstack (lastStakeId, user, user_idn, stackamt, unstackAmt, unstackDate, transaction_id, block_timestamp, block_number) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    await runQuery(insertSql, values);
    console.log(`magicBoosterUnstack saved: ${tx}`);
  } catch (err) {
    console.error("magicBoosterUnstack handler error:", err, { rv, tx });
  }
}

async function handle_claimMBoosterIncome(rv, tx, ts, block) {
  try {
    const userRegId = await safeUserIdn(rv.user);
    const values = [rv.lastStakeId, rv.user, userRegId, rv.stackamt, rv.perdayIncome, rv.unstackDate, tx, ts, block];
    const checkSql = `SELECT id FROM claimMBoosterIncome WHERE lastStakeId=? AND user=? AND user_idn=? AND stackamt=? AND perdayIncome=? AND unstackDate=? AND transaction_id=? AND block_timestamp=? AND block_number=?`;
    const found = await runQuery(checkSql, values);
    if (found && found.length > 0) return;

    const insertSql = `INSERT INTO claimMBoosterIncome (lastStakeId, user, user_idn, stackamt, perdayIncome, unstackDate, transaction_id, block_timestamp, block_number) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    await runQuery(insertSql, values);
    console.log(`claimMBoosterIncome saved: ${tx}`);
  } catch (err) {
    console.error("claimMBoosterIncome handler error:", err, { rv, tx });
  }
}

/* --------------------------
   Your existing DB helpers (preserved)
   -------------------------- */
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

/* --------------------------
   Server
   -------------------------- */
app.get("/", (req, res) => res.send("Server running!"));
app.listen(3000, () => console.log("Server on port 3000"));
