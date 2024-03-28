# from res.connectors.shopify.utils.ShopifyProduct import ShopifyProduct

# BRAND_CODE = "TT"


# class TestShopifyProduct:
#     def test_check_live_status_postivie(self):
#         client = ShopifyProduct(brand_code=BRAND_CODE)
#         product_handle = "sun-daze-blouse"  # actually product live on the testing site -> https://resonance-stores.myshopify.com/products/sun-daze-blouse
#         response = client.check_live_status(product_handle=product_handle)

#         assert response is True

#     def test_check_live_status_negative(self):
#         client = ShopifyProduct(brand_code=BRAND_CODE)
#         product_handle = "false-handler"  # actually product live on the testing site -> https://resonance-stores.myshopify.com/products/sun-daze-blouse
#         response = client.check_live_status(product_handle=product_handle)

#         assert response is False

#     def test_check_product_is_live_positive(self):
#         product_id = "602e05186af438d92eb9087d"
#         client = ShopifyProduct(brand_code=BRAND_CODE)
#         response = client.check_product_is_live(product_id)

#         assert response is True

#     def test_check_product_is_live_negative(self):
#         product_id = "602e00df6af438d92eb9069d"
#         client = ShopifyProduct(brand_code=BRAND_CODE)
#         response = client.check_product_is_live(product_id)

#         assert response is False
