import json
import unittest
from .container import build_container_tree, Container

CONTAINER_STRING = """
{
  "userProfile": {
    "userId": "U12345678",
    "personalDetails": {
      "firstName": "John",
      "lastName": "Doe",
      "contact": {
        "email": "john.doe@example.com",
        "phoneNumbers": [
          {
            "type": "home",
            "number": "555-1234"
          },
          {
            "type": "work",
            "number": "555-5678"
          }
        ]
      }
    },
    "orders": [
      {
        "orderId": "O98765",
        "date": "2025-10-26",
        "items": [
          {
            "itemId": "P101",
            "name": "Laptop",
            "quantity": 1,
            "details": {
              "serialNumber": "SN12345678",
              "warranty": {
                "status": "active",
                "endDate": "2027-10-26"
              }
            }
          },
          {
            "itemId": "P102",
            "name": "Mouse",
            "quantity": 2,
            "details": {
              "serialNumber": "SN87654321",
              "warranty": {
                "status": "inactive",
                "endDate": "2024-05-15"
              }
            }
          }
        ],
        "shippingAddress": {
          "street": "123 Main St",
          "city": "Anytown",
          "zipCode": "12345"
        }
      },
      {
        "orderId": "O65432",
        "date": "2025-11-01",
        "items": [
          {
            "itemId": "P201",
            "name": "Monitor",
            "quantity": 1,
            "details": {
              "serialNumber": "SN45678901",
              "warranty": {
                "status": "active",
                "endDate": "2027-11-01"
              }
            }
          }
        ],
        "shippingAddress": {
          "street": "123 Main St",
          "city": "Anytown",
          "zipCode": "12345"
        }
      }
    ],
    "preferences": {
      "newsletter": true,
      "theme": "dark"
    }
  }
}
"""


GOLDEN_WARRENTY = """{"endDate": "2027-11-01", "status": "active"}"""
GOLDEN_SHIPPING_ADDRESS = """{"street": "123 Main St","city": "Anytown","zipCode": "12345"}"""

class Settings:
    container = None
    def __init__(self, container):
        self.container = container

    def search_container(self, search_string):
        self.container.range_values()

class Test(unittest.TestCase):

    def setUp(self):
        self.json_data = json.loads(CONTAINER_STRING)
        self.container = build_container_tree(start=self.json_data)
        self.golden_warranty = json.loads(GOLDEN_WARRENTY)
        self.golden_shipping_address = json.loads(GOLDEN_SHIPPING_ADDRESS)

    def test_build_container_tree(self):
        container = build_container_tree(start=self.json_data)
        self.assertIsNotNone(container, "token is none")
        self.assertIsInstance(container, Container, "container is not an instance of Container")

    def test_read_primitive_value(self):
        container = build_container_tree(start=self.json_data)
        self.assertEqual(self.golden_warranty, container.read_primitive_value("userProfile.orders.1.items.0.details.warranty"))



if __name__ == '__main__':
    unittest.main()
