# Aeron OMS

## Testing Locally

> Terminal 1 - Aeron Media Driver
> ```bash
> ./gradlew :oms-media-driver:run
>```

> Terminal 2 - OMS 
> ```bash
> ./gradlew :oms-app:run
>```

> Terminal 3 - FIX Gateway
> ```bash
> ./gradlew :oms-fix-client-gateway:fix-acceptor:run
>```

> Terminal 4 - FIX Client
> ```bash
> ./gradlew :oms-fix-client-gateway:fix-client:bootRun
>```

```bash
curl -N -X POST 'http://localhost:8082/api/v1/orders' -H 'accept: text/event-stream' -H 'Content-Type: application/json'   -d '{"symbol":"AAPL","side":"BUY","orderType":"LIMIT","price":185.50,"quantity":100}'
```

