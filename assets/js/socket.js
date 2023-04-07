export class SocketManager {
  constructor (app) {
    this._app = app
    this._hasRequestedHistoryGraph = false
    this._reconnectDelayBase = 0
  }

  reset () {
    this._hasRequestedHistoryGraph = false
  }

  createWebSocket () {
    let webSocketProtocol = 'ws:'
    if (location.protocol === 'https:') {
      webSocketProtocol = 'wss:'
    }

    this._webSocket = new WebSocket(`${webSocketProtocol}//${location.host}`)

    // The backend will automatically push data once connected
    this._webSocket.onopen = () => {
      this._app.caption.set('...מבצע תקשורת')

      // Reset reconnection scheduling since the WebSocket has been established
      this._reconnectDelayBase = 0
    }

    this._webSocket.onclose = (event) => {
      this._app.handleDisconnect()

      // Modify page state to display loading overlay
      // Code 1006 denotes "Abnormal closure", most likely from the server or client losing connection
      // See https://developer.mozilla.org/en-US/docs/Web/API/CloseEvent
      // Treat other codes as active errors (besides connectivity errors) when displaying the message
      if (event.code === 1006) {
        this._app.caption.set('תקשורת אבדה!')
      } else {
        this._app.caption.set('תקשורת אבדה!')
      }

      // Schedule socket reconnection attempt
      this.scheduleReconnect()
    }

    this._webSocket.onmessage = (message) => {
      const payload = JSON.parse(message.data)

      switch (payload.message) {
        case 'init':
          this._app.setPublicConfig(payload.config)

          // Display the main page component
          // Called here instead of syncComplete so the DOM can be drawn prior to the graphs being drawn
          this._app.setPageReady(true)

          // Allow the graphDisplayManager to control whether or not the historical graph is loaded
          // Defer to isGraphVisible from the publicConfig to understand if the frontend will ever receive a graph payload
          if (this._app.publicConfig.isGraphVisible) {
            this.sendHistoryGraphRequest()
          }

          payload.servers.forEach((serverPayload, serverId) => {
            this._app.addServer(serverId, serverPayload, payload.timestampPoints)
          })

          // Init payload contains all data needed to render the page
          // Alert the app it is ready
          this._app.handleSyncComplete()

          break

        case 'updateServers': {
          for (let serverId = 0; serverId < payload.updates.length; serverId++) {
            // The backend may send "update" events prior to receiving all "add" events
            // A server has only been added once it's ServerRegistration is defined
            // Checking undefined protects from this race condition
            const serverRegistration = this._app.serverRegistry.getServerRegistration(serverId)
            const serverUpdate = payload.updates[serverId]

            if (serverRegistration) {
              serverRegistration.handlePing(serverUpdate, payload.timestamp)
              serverRegistration.updateServerStatus(serverUpdate, this._app.publicConfig.minecraftVersions)
            }
          }

          // Bulk add playerCounts into graph during #updateHistoryGraph
          if (payload.updateHistoryGraph) {
            this._app.graphDisplayManager.addGraphPoint(payload.timestamp, Object.values(payload.updates).map(update => update.playerCount))

            // Run redraw tasks after handling bulk updates
            this._app.graphDisplayManager.redraw()
          }

          this._app.updateGlobalStats()

          break
        }

        case 'historyGraph': {
          this._app.graphDisplayManager.buildPlotInstance(payload.timestamps, payload.graphData)
          break
        }
      }
    }
  }

  scheduleReconnect () {
    // Release any active WebSocket references
    this._webSocket = undefined

    this._reconnectDelayBase++

    // Exponential backoff for reconnection attempts
    // Clamp ceiling value to 30 seconds
    this._reconnectDelaySeconds = Math.min((this._reconnectDelayBase * this._reconnectDelayBase), 30)

    const reconnectInterval = setInterval(() => {
      this._reconnectDelaySeconds--

      if (this._reconnectDelaySeconds === 0) {
        // Explicitly clear interval, this avoids race conditions
        // #clearInterval first to avoid potential errors causing pre-mature returns
        clearInterval(reconnectInterval)

        // Update displayed text
        this._app.caption.set('Reconnecting...')

        // Attempt reconnection
        // Only attempt when reconnectDelaySeconds === 0 and not <= 0, otherwise multiple attempts may be started
        this.createWebSocket()
      } else if (this._reconnectDelaySeconds > 0) {
        // Update displayed text
        this._app.caption.set(`Reconnecting in ${this._reconnectDelaySeconds}s...`)
      }
    }, 1000)
  }

  sendHistoryGraphRequest () {
    if (!this._hasRequestedHistoryGraph) {
      this._hasRequestedHistoryGraph = true

      // Send request as a plain text string to avoid the server needing to parse JSON
      // This is mostly to simplify the backend server's need for error handling
      this._webSocket.send('requestHistoryGraph')
    }
  }
}
