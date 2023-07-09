const mysql = require("mysql");
const logger = require("./logger");
const config = require("../config");
const { TimeTracker } = require("./time");

class Database {
  constructor(app) {
    this._app = app;
    this._sql = mysql.createConnection({
      host: config.database.host,
      user: config.database.username,
      password: config.database.password,
      database: config.database.database,
      port: config.database.port,
    });

    this._sql.connect((err) => {
      if (err) {
        logger.error("Cannot connect to MySQL database");
        throw err;
      }
      logger.info("Connected to MySQL database");
    });
  }

  ensureIndexes(callback) {
    const handleError = (err) => {
      if (err) {
        logger.log("error", "Cannot create table or table index");
        throw err;
      }
    };

    this._sql.query(
      "CREATE TABLE IF NOT EXISTS pings (timestamp BIGINT NOT NULL, ip TINYTEXT, playerCount MEDIUMINT)",
      handleError
    );
    this._sql.query(
      "CREATE TABLE IF NOT EXISTS players_record (timestamp BIGINT, ip VARCHAR(255), playerCount MEDIUMINT, PRIMARY KEY (ip(255)))",
      handleError
    );

    const addColumnQuery = "ALTER TABLE pings ADD COLUMN ip_hash BINARY(16)";
    this._sql.query(addColumnQuery, (err) => {
      if (err) {
        logger.error("Error adding column 'ip_hash'");
      }

      const updateQuery = "UPDATE pings SET ip_hash = UNHEX(MD5(ip))";
      this._sql.query(updateQuery, (err) => {
        if (err) {
          logger.error("Error updating 'ip_hash'");
        }
      });
    });

    this._sql.query("UPDATE pings SET ip_hash = UNHEX(MD5(ip))", handleError);
    this._sql.query("DROP INDEX ip_index ON pings", (err) => {
      if (err) {
        logger.error("Error dropping index 'ip_index'");
      }
    });
    this._sql.query(
      "CREATE INDEX ip_index ON pings (ip_hash, playerCount)",
      handleError
    );

    this._sql.query("DROP INDEX timestamp_index ON pings", (err) => {
      if (err) {
        logger.error("Error dropping index 'timestamp_index'");
      }
    });

    this._sql.query(
      "CREATE INDEX timestamp_index ON pings (timestamp)",
      [],
      (err) => {
        handleError(err);
        callback();
      }
    );

    callback();
  }

  loadGraphPoints(graphDuration, callback) {
    // Query recent pings
    const endTime = TimeTracker.getEpochMillis();
    const startTime = endTime - graphDuration;

    this.getRecentPings(startTime, endTime, (pingData) => {
      const relativeGraphData = [];

      for (const row of pingData) {
        // Load into temporary array
        // This will be culled prior to being pushed to the serverRegistration
        let graphData = relativeGraphData[row.ip];
        if (!graphData) {
          relativeGraphData[row.ip] = graphData = [[], []];
        }

        // DANGER!
        // This will pull the timestamp from each row into memory
        // This is built under the assumption that each round of pings shares the same timestamp
        // This enables all timestamp arrays to have consistent point selection and graph correctly
        graphData[0].push(row.timestamp);
        graphData[1].push(row.playerCount);
      }

      Object.keys(relativeGraphData).forEach((ip) => {
        // Match IPs to serverRegistration object
        for (const serverRegistration of this._app.serverRegistrations) {
          if (serverRegistration.data.ip === ip) {
            const graphData = relativeGraphData[ip];

            // Push the data into the instance and cull if needed
            serverRegistration.loadGraphPoints(
              startTime,
              graphData[0],
              graphData[1]
            );

            break;
          }
        }
      });

      // Since all timestamps are shared, use the array from the first ServerRegistration
      // This is very dangerous and can break if data is out of sync
      if (Object.keys(relativeGraphData).length > 0) {
        const serverIp = Object.keys(relativeGraphData)[0];
        const timestamps = relativeGraphData[serverIp][0];

        this._app.timeTracker.loadGraphPoints(startTime, timestamps);
      }

      callback();
    });
  }

  loadRecords(callback) {
    let completedTasks = 0;

    this._app.serverRegistrations.forEach((serverRegistration) => {
      serverRegistration.findNewGraphPeak();

      this.getRecord(
        serverRegistration.data.ip,
        (hasRecord, playerCount, timestamp) => {
          if (hasRecord) {
            serverRegistration.recordData = {
              playerCount,
              timestamp: TimeTracker.toSeconds(timestamp),
            };
          } else {
            this.getRecordLegacy(
              serverRegistration.data.ip,
              (hasRecordLegacy, playerCountLegacy, timestampLegacy) => {
                let newTimestamp = null;
                let newPlayerCount = null;

                if (hasRecordLegacy) {
                  newTimestamp = timestampLegacy;
                  newPlayerCount = playerCountLegacy;
                }

                serverRegistration.recordData = {
                  playerCount: newPlayerCount,
                  timestamp: TimeTracker.toSeconds(newTimestamp),
                };

                const insertQuery =
                  "INSERT IGNORE INTO players_record (timestamp, ip, playerCount) VALUES (?, ?, ?)";
                const values = [
                  newTimestamp,
                  serverRegistration.data.ip,
                  newPlayerCount,
                ];

                this._sql.query(insertQuery, values, (err) => {
                  if (err && err.code !== "ER_DUP_ENTRY") {
                    logger.error("Error inserting player count record");
                    throw err;
                  }
                });
              }
            );
          }
        }
      );
    });
  }

  getRecentPings(startTime, endTime, callback) {
    this._sql.query(
      "SELECT * FROM pings WHERE timestamp >= ? AND timestamp <= ?",
      [startTime, endTime],
      (err, data) => {
        if (err) {
          logger.log("error", "Cannot get recent pings");
          throw err;
        }
        callback(data);
      }
    );
  }

  getRecord(ip, callback) {
    this._sql.query(
      "SELECT playerCount, timestamp FROM players_record WHERE ip = ?",
      [ip],
      (err, data) => {
        if (err) {
          logger.log("error", `Cannot get ping record for ${ip}`);
          throw err;
        }

        // Record not found
        if (data[0] === undefined) {
          callback(false);
          return;
        }

        const playerCount = data[0].playerCount;
        const timestamp = data[0].timestamp;

        callback(true, playerCount, timestamp);
      }
    );
  }

  getRecordLegacy(ip, callback) {
    this._sql.query(
      "SELECT MAX(playerCount) as maxPlayerCount, timestamp FROM pings WHERE ip = 'play.topstrix.net' GROUP BY timestamp",
      (err, data) => {
        if (err) {
          logger.log("error", `Cannot get legacy ping record for ${ip}`);
          throw err;
        }

        // For empty results, data will be an empty array []
        if (data.length > 0) {
          const playerCount = data[0].maxPlayerCount;
          const timestamp = data[0].timestamp;

          // eslint-disable-next-line node/no-callback-literal
          callback(true, playerCount, timestamp);
        } else {
          // eslint-disable-next-line node/no-callback-literal
          callback(false);
        }
      }
    );
  }

  insertPing(ip, timestamp, unsafePlayerCount) {
    this._insertPingTo(ip, timestamp, unsafePlayerCount, this._sql);
  }

  _insertPingTo(ip, timestamp, unsafePlayerCount, db) {
    const sqlQuery =
      "INSERT INTO pings (timestamp, ip, playerCount) VALUES (?, ?, ?)";
    const values = [timestamp, ip, unsafePlayerCount];

    db.query(sqlQuery, values, (err) => {
      if (err) {
        logger.error(`Cannot insert ping record of ${ip} at ${timestamp}`);
        throw err;
      }
    });
  }

  updatePlayerCountRecord(ip, playerCount, timestamp) {
    const sqlQuery =
      "UPDATE players_record SET timestamp = ?, playerCount = ? WHERE ip = ?";
    const values = [timestamp, playerCount, ip];

    this._sql.query(sqlQuery, values, (err) => {
      if (err) {
        logger.error(
          `Cannot update player count record of ${ip} at ${timestamp}`
        );
        throw err;
      }
    });
  }

  initOldPingsDelete(callback) {
    // Delete old pings on startup
    logger.info("Deleting old pings..");
    this.deleteOldPings(() => {
      const oldPingsCleanupInterval =
        config.oldPingsCleanup.interval || 3600000;
      if (oldPingsCleanupInterval > 0) {
        // Delete old pings periodically
        setInterval(() => this.deleteOldPings(), oldPingsCleanupInterval);
      }

      callback();
    });
  }

  deleteOldPings(callback) {
    const oldestTimestamp = TimeTracker.getEpochMillis() - config.graphDuration;

    const deleteStart = TimeTracker.getEpochMillis();
    const statement = this._sql.prepare(
      "DELETE FROM pings WHERE timestamp < ?;"
    );
    statement.run(oldestTimestamp, (err) => {
      if (err) {
        logger.error("Cannot delete old pings");
        throw err;
      } else {
        const deleteTook = TimeTracker.getEpochMillis() - deleteStart;
        logger.info(`Old pings deleted in ${deleteTook}ms`);

        if (callback) {
          callback();
        }
      }
    });
    statement.finalize();
  }

  // Close the MySQL connection when no longer needed
  close() {
    this._sql.end();
  }
}

module.exports = Database;
