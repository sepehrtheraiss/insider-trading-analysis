import pytest
from unittest.mock import MagicMock
from api.client.services.core_service import SecClient

@pytest.fixture
def lib_fake_client():
    """Mock LibraryClient-like object."""
    fake = MagicMock()
    fake.fetch.return_value = {
        "transactions": [
            {"ticker": "AAPL", "shares": 200, "price": 190.5},
            {"ticker": "MSFT", "shares": 300, "price": 310.0},
        ]
    }
    return fake

@pytest.fixture
def http_fake_client():
    """Mock LibraryClient-like object."""
    fake = MagicMock()
    fake.fetch.return_value = [{'name': 'ADMIRALTY BANCORP INC', 'ticker': 'AAAB', 'cik': '1066808', 'cusip': '007231103',
                                'exchange': 'NASDAQ', 'isDelisted': True, 'category': 'Domestic Common Stock',
                                'sector': 'Financial Services', 'industry': 'Banks - Regional', 'sic': '6022',
                                'sicSector': 'Finance Insurance And Real Estate', 'sicIndustry': 'State Commercial Banks',
                                'famaSector': '', 'famaIndustry': 'Banking', 'currency': 'USD', 'location': 'Florida; U.S.A',
                                'id': '3daf92ec6639eede5c282d6cd20f8342'},
                                {'name': 'ADVANCED ACCELERATOR APPLICATIONS SA',
                                'ticker': 'AAAP', 'cik': '1611787', 'cusip': '00790T100', 'exchange': 'NASDAQ', 'isDelisted': True,
                                'category': 'ADR Common Stock', 'sector': 'Healthcare', 'industry': 'Biotechnology', 'sic': '2834',
                                'sicSector': 'Manufacturing', 'sicIndustry': 'Pharmaceutical Preparations', 'famaSector': '',
                                'famaIndustry': 'Pharmaceutical Products', 'currency': 'EUR', 'location': 'France',
                                'id': 'cf1185a716b218ee7db7d812695108eb'}]
    return fake

@pytest.fixture
def sec_client(lib_fake_client, http_fake_client):
    """Inject mock client into CoreService."""
    return SecClient(lib_client=lib_fake_client, http_client=http_fake_client)
