const { MongoClient } = require("mongodb");
const logger = require("./logger");
const config = require("../config");
const { TimeTracker } = require("./time");

class Database {
  constructor(app) {
    this._app = app;
    this._client = null;
    this._db = null;
  }

  async connect() {
    try {
      this._client = await MongoClient.connect(config.database.connection_url);
      this._db = this._client.db();
    } catch (err) {
      logger.error("Failed to connect to MongoDB");
      throw err;
    }
  }

  async ensureIndexes(callback) {
    if (!this._db) {
      throw new Error("Database connection not established");
    }

    try {
      const pingsCollection = this._db.collection("pings");
      const playersRecordCollection = this._db.collection("players_record");

      // Create indexes
      await pingsCollection.createIndex({ ip: 1, playerCount: 1 });
      await pingsCollection.createIndex({ timestamp: 1 });
      await playersRecordCollection.createIndex({ ip: 1 });

      callback();
    } catch (err) {
      logger.error("Cannot create table or table index");
      throw err;
    }
  }

  async loadGraphPoints(graphDuration, callback) {
    const endTime = TimeTracker.getEpochMillis();
    const startTime = endTime - graphDuration;

    try {
      const pingData = await this.getRecentPings(startTime, endTime);
      const relativeGraphData = {};

      for (const row of pingData) {
        let graphData = relativeGraphData[row.ip];
        if (!graphData) {
          relativeGraphData[row.ip] = graphData = [[], []];
        }

        graphData[0].push(row.timestamp);
        graphData[1].push(row.playerCount);
      }

      for (const ip of Object.keys(relativeGraphData)) {
        const graphData = relativeGraphData[ip];

        for (const serverRegistration of this._app.serverRegistrations) {
          if (serverRegistration.data.ip === ip) {
            serverRegistration.loadGraphPoints(
              startTime,
              graphData[0],
              graphData[1]
            );
            break;
          }
        }
      }

      if (Object.keys(relativeGraphData).length > 0) {
        const serverIp = Object.keys(relativeGraphData)[0];
        const timestamps = relativeGraphData[serverIp][0];
        this._app.timeTracker.loadGraphPoints(startTime, timestamps);
      }

      callback();
    } catch (err) {
      logger.error("Failed to load graph points");
      throw err;
    }
  }

  async loadRecords(callback) {
    let completedTasks = 0;

    for (const serverRegistration of this._app.serverRegistrations) {
      serverRegistration.findNewGraphPeak();

      try {
        const record = await this.getRecord(serverRegistration.data.ip);
        if (record) {
          serverRegistration.recordData = {
            playerCount: record.playerCount,
            timestamp: TimeTracker.toSeconds(record.timestamp),
          };
        } else {
          const legacyRecord = await this.getRecordLegacy(
            serverRegistration.data.ip
          );
          let newTimestamp = null;
          let newPlayerCount = null;

          if (legacyRecord) {
            newTimestamp = legacyRecord.timestamp;
            newPlayerCount = legacyRecord.playerCount;
          }

          serverRegistration.recordData = {
            playerCount: newPlayerCount,
            timestamp: TimeTracker.toSeconds(newTimestamp),
          };

          await this.insertRecord(
            serverRegistration.data.ip,
            newPlayerCount,
            newTimestamp
          );
        }

        completedTasks++;
        if (completedTasks === this._app.serverRegistrations.length) {
          callback();
        }
      } catch (err) {
        logger.error(
          `Failed to load records for ${serverRegistration.data.ip}`
        );
        throw err;
      }
    }
  }

  async getRecentPings(startTime, endTime) {
    try {
      const pingsCollection = this._db.collection("pings");
      const query = { timestamp: { $gte: startTime, $lte: endTime } };
      return await pingsCollection.find(query).toArray();
    } catch (err) {
      logger.error("Failed to get recent pings");
      throw err;
    }
  }

  async getRecord(ip) {
    try {
      const playersRecordCollection = this._db.collection("players_record");
      const query = { ip };
      return await playersRecordCollection.findOne(query);
    } catch (err) {
      logger.error(`Failed to get ping record for ${ip}`);
      throw err;
    }
  }

  async getRecordLegacy(ip) {
    try {
      const pingsCollection = this._db.collection("pings");
      const query = { ip };
      const sort = { playerCount: -1 };
      return await pingsCollection.findOne(query, { sort });
    } catch (err) {
      logger.error(`Failed to get legacy ping record for ${ip}`);
      throw err;
    }
  }

  async insertPing(ip, timestamp, unsafePlayerCount) {
    try {
      const pingsCollection = this._db.collection("pings");
      const document = { timestamp, ip, playerCount: unsafePlayerCount };
      await pingsCollection.insertOne(document);
    } catch (err) {
      logger.error(`Failed to insert ping record of ${ip} at ${timestamp}`);
      throw err;
    }
  }

  async insertRecord(ip, playerCount, timestamp) {
    try {
      const playersRecordCollection = this._db.collection("players_record");
      const document = { timestamp, ip, playerCount };
      await playersRecordCollection.insertOne(document);
    } catch (err) {
      logger.error(
        `Failed to insert player count record of ${ip} at ${timestamp}`
      );
      throw err;
    }
  }

  async updatePlayerCountRecord(ip, playerCount, timestamp) {
    try {
      const playersRecordCollection = this._db.collection("players_record");
      const query = { ip };
      const update = { $set: { playerCount, timestamp } };
      await playersRecordCollection.updateOne(query, update);
    } catch (err) {
      logger.error(
        `Cannot update player count record of ${ip} at ${timestamp}`
      );
      throw err;
    }
  }

  async initOldPingsDelete(callback) {
    logger.info("Deleting old pings..");
    await this.deleteOldPings();

    const oldPingsCleanupInterval = config.oldPingsCleanup.interval || 3600000;
    if (oldPingsCleanupInterval > 0) {
      setInterval(() => this.deleteOldPings(), oldPingsCleanupInterval);
    }

    callback();
  }

  async deleteOldPings() {
    const oldestTimestamp = TimeTracker.getEpochMillis() - config.graphDuration;

    try {
      const pingsCollection = this._db.collection("pings");
      const query = { timestamp: { $lt: oldestTimestamp } };
      await pingsCollection.deleteMany(query);
    } catch (err) {
      logger.error("Failed to delete old pings");
      throw err;
    }
  }
}

module.exports = Database;
