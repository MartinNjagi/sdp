#!/bin/bash

set -e

mkdir -p sdp/{connections,models,router}
mkdir -p sdp/controllers/{publisher,worker,wallet,mno_router,ratelimiter,dispatcher,dlr}

touch sdp/main.go

touch sdp/connections/config.go
touch sdp/models/envelope.go
touch sdp/router/app.go

touch sdp/controllers/sdp.go

touch sdp/controllers/publisher/publisher.go

touch sdp/controllers/worker/worker.go
touch sdp/controllers/worker/bulk_worker.go
touch sdp/controllers/worker/dispatch_worker.go

touch sdp/controllers/wallet/wallet.go
touch sdp/controllers/wallet/lua.go
touch sdp/controllers/wallet/flusher.go

touch sdp/controllers/mno_router/mno_router.go

touch sdp/controllers/ratelimiter/ratelimiter.go

touch sdp/controllers/dispatcher/dispatcher.go
touch sdp/controllers/dispatcher/http.go
touch sdp/controllers/dispatcher/smpp.go

touch sdp/controllers/dlr/reconciler.go
touch sdp/controllers/dlr/handler.go

echo "SDP project structure created successfully."