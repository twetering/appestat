"""
Albert Heijn API client for Appestat.
Handles OAuth2 authentication and receipt fetching via GraphQL.

Auth flow:
1. User opens AUTH_URL in browser and logs in with AH credentials
2. AH redirects to appie://login-exit?code=XXXX
3. User pastes the full redirect URL (or just the code) into Appestat
4. We exchange the code for access + refresh tokens
"""

import json
import time
from pathlib import Path
from typing import Optional

import httpx

TOKEN_FILE = Path(__file__).parent.parent / "data" / "ah_tokens.json"
BASE_URL = "https://api.ah.nl"
USER_AGENT = "Appie/9.27.0"
AUTH_URL = (
    "https://login.ah.nl/secure/oauth/authorize"
    "?client_id=appie"
    "&redirect_uri=appie://login-exit"
    "&response_type=code"
)

_GET_RECEIPTS_QUERY = """
query GetReceipts($pagination: OffsetLimitPagination!) {
    posReceiptsPage(pagination: $pagination) {
        pagination {
            offset
            limit
            totalElements
        }
        posReceipts {
            id
            dateTime
            totalAmount {
                amount
                formatted
            }
            storeAddress {
                city
                street
            }
        }
    }
}
"""

_GET_RECEIPT_QUERY = """
query GetReceipt($id: String!) {
    posReceiptDetails(id: $id) {
        id
        memberId
        storeInfo
        products {
            id
            name
            quantity
            price {
                amount
                formatted
            }
            amount {
                amount
                formatted
            }
        }
        subtotalProducts {
            amount {
                amount
                formatted
            }
        }
        discounts {
            type
            name
            amount {
                amount
                formatted
            }
        }
        discountTotal {
            amount
            formatted
        }
        total {
            amount
            formatted
        }
        payments {
            method
            amount {
                amount
                formatted
            }
        }
        transaction {
            dateTime
            store
            lane
            id
        }
        address {
            street
            city
            postalCode
        }
        vat {
            levels {
                percentage
                amount {
                    amount
                    formatted
                }
            }
            total {
                amount {
                    amount
                    formatted
                }
            }
        }
    }
}
"""


class AHApiClient:
    """Synchronous Albert Heijn API client."""

    _instance: Optional["AHApiClient"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._token_expiry: Optional[float] = None
        self._load_tokens()

    def _load_tokens(self):
        if TOKEN_FILE.exists():
            try:
                data = json.loads(TOKEN_FILE.read_text())
                self._access_token = data.get("access_token")
                self._refresh_token = data.get("refresh_token")
                self._token_expiry = data.get("expiry")
            except (json.JSONDecodeError, IOError):
                pass

    def _save_tokens(self):
        TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
        TOKEN_FILE.write_text(json.dumps({
            "access_token": self._access_token,
            "refresh_token": self._refresh_token,
            "expiry": self._token_expiry,
        }))

    @property
    def is_authenticated(self) -> bool:
        return self._access_token is not None

    def _is_token_expired(self) -> bool:
        if not self._token_expiry:
            return False
        return time.time() > self._token_expiry - 60

    @staticmethod
    def get_auth_url() -> str:
        return AUTH_URL

    def exchange_code(self, code: str) -> dict:
        """Exchange authorization code for access + refresh tokens."""
        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                f"{BASE_URL}/mobile-auth/v1/auth/token",
                headers={"User-Agent": USER_AGENT, "Content-Type": "application/json"},
                json={"clientId": "appie", "code": code},
            )
            response.raise_for_status()
            data = response.json()
            self._access_token = data.get("access_token")
            self._refresh_token = data.get("refresh_token")
            expires_in = data.get("expires_in", 7200)
            self._token_expiry = time.time() + expires_in
            self._save_tokens()
            return data

    def refresh_tokens(self) -> dict:
        if not self._refresh_token:
            raise ValueError("No refresh token available")
        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                f"{BASE_URL}/mobile-auth/v1/auth/token/refresh",
                headers={"User-Agent": USER_AGENT, "Content-Type": "application/json"},
                json={"clientId": "appie", "refreshToken": self._refresh_token},
            )
            response.raise_for_status()
            data = response.json()
            self._access_token = data.get("access_token")
            self._refresh_token = data.get("refresh_token")
            expires_in = data.get("expires_in", 7200)
            self._token_expiry = time.time() + expires_in
            self._save_tokens()
            return data

    def clear_tokens(self):
        self._access_token = None
        self._refresh_token = None
        self._token_expiry = None
        self._initialized = False
        AHApiClient._instance = None
        if TOKEN_FILE.exists():
            TOKEN_FILE.unlink()

    def _ensure_valid_token(self):
        if self._is_token_expired() and self._refresh_token:
            self.refresh_tokens()

    def _headers(self) -> dict:
        headers = {
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self._access_token:
            headers["Authorization"] = f"Bearer {self._access_token}"
        return headers

    def _graphql(self, query: str, variables: dict = None) -> dict:
        self._ensure_valid_token()
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                f"{BASE_URL}/graphql",
                headers=self._headers(),
                json=payload,
            )
            if response.status_code == 401 and self._refresh_token:
                self.refresh_tokens()
                response = client.post(
                    f"{BASE_URL}/graphql",
                    headers=self._headers(),
                    json=payload,
                )
            response.raise_for_status()
            result = response.json()
            if "errors" in result:
                raise Exception(f"GraphQL errors: {result['errors']}")
            return result.get("data", {})

    def get_receipts(self, offset: int = 0, limit: int = 20) -> dict:
        """Fetch paginated list of receipt summaries."""
        data = self._graphql(_GET_RECEIPTS_QUERY, {"pagination": {"offset": offset, "limit": limit}})
        return data.get("posReceiptsPage", {})

    def get_receipt(self, receipt_id: str) -> dict:
        """Fetch full receipt details including products."""
        data = self._graphql(_GET_RECEIPT_QUERY, {"id": receipt_id})
        return data.get("posReceiptDetails", {})
