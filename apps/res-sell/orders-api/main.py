import os

from flasgger import Swagger

from res.utils import logger
from res.utils.flask.ResFlask import ResFlask


__version__ = 0.1

app = ResFlask(
    __name__,
    enable_trace_logging=True,
    enable_cognito=False,
)
template = {
    "swagger": "2.0",
    "info": {
        "title": "Resonance Orders API",
        "description": "API for Brands to send order requests to Resonance for production",
        "contact": {
            "responsibleOrganization": "Resonance Engineering",
            # "responsibleDeveloper": "one-engineering@resonance.nyc",
            "email": "one-engineering@resonance.nyc",
            # "url": "resonance.nyc",
        },
        # "termsOfService": "http://resonance.nyc/terms",
        "version": "0.1.1",
    },
    "host": "data.resmagic.io"
    if os.getenv("RES_ENV", "development") == "development"
    else "datadev.resmagic.io",  # overrides localhost:500
    # "basePath": "/api",  # base bash for blueprint registration
    "schemes": ["https"],
    "operationId": "getmyData",
}
swagger_config = {
    "headers": [],
    "specs": [
        {
            "endpoint": "apispec_1",
            "route": "/orders-api/apispec_1.json",
            "rule_filter": lambda rule: True,  # all in
            "model_filter": lambda tag: True,  # all in
        }
    ],
    "static_url_path": "/orders-api/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/orders-api/apidocs/",
}
swagger = Swagger(app, config=swagger_config, template=template)


@app.route("/orders-api/orders/create", methods=["POST"])
def create():
    """Create a new Resonance Order
    Order is created in the Resonance system, and will be produced and send to the customer
    ---
    parameters:
      - name: body
        in: body
        required: true
        schema:
          $ref: "#/definitions/Order"
    definitions:
      OrderID:
        type: object
        properties:
          id:
            type: string
      Order:
        type: object
        properties:
          id:
            type: string
            required: false
            description: Resonance ID for the order. Will be ignored if provided for new orders.
          status:
            type: string
            required: false
            description: Set this status field to "CANCELED" to cancel an order. Will show current status, of "CREATED"/"CANCELED"/"IN_PROGRESS"/"FULFILLED"
          internal_order_id:
            type: string
            required: false
            description: Your internal ID for tracking orders, separate from the ID Resonance provides
          customer:
            type: object
            description: Customer object
            required: true
            type: object
            properties:
              address1:
                type: string
                required: true
              address2:
                type: string
                required: true
              city:
                type: string
                required: true
              country:
                type: string
                required: true
              province:
                type: string
                required: true
              province_code:
                type: string
                required: true
              zip:
                type: string
                required: true
              first_name:
                type: string
                required: true
              last_name:
                type: string
                required: true
              email:
                type: string
                required: true
              phone:
                type: string
                required: false
          line_items:
            type: array
            description: list of line items in order
            required: true
            items:
              $ref: "#/definitions/LineItem"
          order_timestamp:
            type: string
            required: true
            description: Timestamp order was placed, in YYYY-MM-DD HH:MM:SS format in UTC timezone.
      LineItem:
        type: object
        properties:
          price:
            type: number
            required: true
            description: Price of Item
          sku:
            type: string
            required: true
            description: Resonance SKU for Item
          metadata:
            type: object
            required: false
            description: Item-specific metadata
    responses:
      201:
        description: Order successfully created
        schema:
          $ref: '#/definitions/OrderID'
      400:
        description: Bad Request - Invalid Order
    """
    return {"id": 123}, 201


@app.route("/orders-api/orders/update", methods=["POST"])
def update():
    """Update a Resonance Order
    Given an Order ID and other params, order is updated in the Resonance system
    ---
    parameters:
      - name: body
        in: body
        required: true
        schema:
          $ref: "#/definitions/Order"
    responses:
      201:
        description: Order successfully updated
        schema:
          $ref: '#/definitions/OrderID'
      400:
        description: Bad Request - Invalid Order
    """
    logger.info("Handing update event")
    return {"success": True}, 200


@app.route("/orders-api/orders", methods=["GET"])
def orders():
    """Get a Resonance Order
    Order for given ID is returned
    ---
    parameters:
      - name: body
        in: body
        required: true
        schema:
          $ref: "#/definitions/OrderID"
    responses:
      201:
        description: Order Information
        schema:
          $ref: '#/definitions/Order'
      400:
        description: Bad Request - Invalid OrderID
    """
    logger.info("Handing order event")
    return {"success": True}, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0")
