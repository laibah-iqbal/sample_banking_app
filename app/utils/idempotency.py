import hashlib
import json
from fastapi import HTTPException, status
from app.routers import data_models
from redis.asyncio import Redis

class IdompotencyManager:
    def __init__(self, redis_client: Redis, ttl: int = 86400):
        self.redis = redis_client
        self.ttl = ttl  # Default 24 hours
    
    async def _compute_payload_hash(self, transaction: data_models.TransactionBase) -> str:
        """Hash the request body to prevent payload tampering under the same key."""
        body = transaction.model_dump_json().encode("utf-8")
        return hashlib.sha256(body).hexdigest()
    
    async def check_and_cache(self, idempotency_key: str, transaction: data_models.TransactionBase):
        """Returns redis_key, if key was already in cache, and cached_response"""
        # hash body
        redis_key = f"idempotency:transaction:{idempotency_key}"
        hashed_request = await self._compute_payload_hash(transaction=transaction)
        
        # returns OK if key did not exist and was inserted, returns NULL if key already existed
        key_inserted = await self.redis.set(redis_key,
                                      json.dumps({"status": "PROCESSING", "payload_hash": hashed_request}),
                                      nx=True,
                                      ex=self.ttl)
        
        if key_inserted:
            return redis_key, False, None
        
        cached_data_str = await self.redis.get(redis_key)
        # this is for the chance that the key expired between 
        # insertion check and getting its value
        if not cached_data_str:
            return redis_key, False, None
        
        cached_data = json.loads(cached_data_str)
        # if request body has been changed raise exception
        if hashed_request != cached_data.get("payload_hash"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Idempotency Key Payload Mismatch: Request body changed."
            )
        # if processing, raise exception
        if cached_data.get("status") == "PROCESSING":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="A request with this idempotency key is currently processing."
            )
        
        # return cached response if body hasn't changed and transaction is completed
        return redis_key, True, cached_data.get("response")
    
    async def cache_response(self, redis_key: str, status_code: int, response_body: dict):
        """Save the completed response payload in Redis."""
        data = {
            "status": "COMPLETED",
            "response": {
                "status_code": status_code,
                "body": response_body
            }
        }
        await self.redis.set(redis_key, json.dumps(data), ex=self.ttl)

    async def release_lock(self, redis_key: str):
        """Remove the key on server error so the user can retry."""
        await self.redis.delete(redis_key)