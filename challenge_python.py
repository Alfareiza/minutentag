from typing import Generator, Any, Iterable
import logging

import requests
from requests import HTTPError, Timeout
import boto3

"""
Refactor the next function using yield to return the array of objects found by the
`s3.list_objects_v2` function that matches the given prefix.
"""


def get_s3_objects(bucket: str, prefix: str = '') -> Generator:
    """ Generator that reach buckets in S3. """
    s3 = boto3.client('s3')

    kwargs = {'Bucket': bucket}
    next_token = None
    if prefix:
        kwargs['Prefix'] = prefix
    # object_list = []  # Omitting this line
    while True:
        if next_token:
            kwargs['ContinuationToken'] = next_token
        resp = s3.list_objects_v2(**kwargs)
        contents = resp.get('Contents', [])
        for obj in contents:
            key = obj['Key']
            if key.startswith(prefix):
                # object_list.append(obj)  # Omitting this line
                yield obj
        next_token = resp.get('NextContinuationToken', None)

        if not next_token:
            break
    # return object_list  # Omitting this line


"""
Please, full explain this function: document iterations, conditionals, and the
function as a whole
"""


def fn(main_plan: Any, obj: dict, extensions: Iterable[dict] = []) -> list[dict]:
    """
    Organize the information products considering the content of main_plan and what
    is coming in extensions
    """
    items = []
    sp = False
    cd = False

    # Create a dict comprehension with id of price and quantity
    # based of information coming from extensions
    ext_p = {ext['price'].id: ext['qty'] for ext in extensions}

    # Reach the data (list of objects) from the items in the obj dictionary
    # and creates a dict with the id of the product
    # Ex.: obj = {
    #       'items': new_obj  # This object has an attr called data that is an iterable of objects.
    # }
    for item in obj['items'].data:
        product = {
            'id': item.id
        }

        # Given the product dictionary
        # - Add up 'deleted' key if either is not coming in extensions or its qty is less than 1
        # - Add up 'qty' already extracted, from ext_p
        if item.price.id != main_plan.id and item.price.id not in ext_p:
            # Adding up the key deleted if it's not coming in extensions
            product['deleted'] = True
            cd = True
        elif item.price.id in ext_p:
            # Taking data from ext_p to increase it into product
            # Item will always be excluded from ext_p
            qty = ext_p[item.price.id]
            if qty < 1:
                product['deleted'] = True
            else:
                product['qty'] = qty
            del ext_p[item.price.id]
        elif item.price.id == main_plan.id:
            sp = True

        items.append(product)
    # Add an extra product if the flag 'sp' is False,
    # which means it relates to the 'main_plan' object
    if not sp:
        items.append({
            'id': main_plan.id,
            'qty': 1
        })

    # Takes the information left in ext_p and adds it
    # to items just if the qt is higher than 1
    for price, qty in ext_p.items():
        if qty < 1:
            continue
        items.append({
            'id': price,
            'qty': qty
        })

    return items


"""
Having the class `Caller` and the function `fn`
Refactor the function `fn` to execute any method from `Caller` using the argument `fn_to_call`
reducing the `fn` function to only one line.
"""

# Solution
fn = lambda name, *args: getattr(Caller, name)(*args)


# Example:
fn('concat', 1, 1)
fn('add', 2, 4)


class Caller:
    add = lambda a, b: a + b
    concat = lambda a, b: f'{a},{b}'
    divide = lambda a, b: a / b
    multiply = lambda a, b: a * b


# def fn(fn_to_call, *args):
#     result = None
#
#     if fn_to_call == 'add':
#         result = Caller.add(*args)
#     if fn_to_call == 'concat':
#         result = Caller.concat(*args)
#     if fn_to_call == 'divide':
#         result = Caller.divide(*args)
#     if fn_to_call == 'multiply':
#         result = Caller.multiply(*args)
#
#     return result


"""
A video transcoder was implemented with different presets to process different videos in the application. The videos should be
encoded with a given configuration done by this function. Can you explain what this function is detecting from the params
and returning based in its conditionals?

R/ The 'ar' variable depicts the screen resolution:
So:
 - if it is less than 1, is vertical,
 - If it is higher than 1.3, is horizontal,
 - Otherwise, is because is between 1.01 and 1.32,

So, being a portrait or landscape or none of them, it will return
a filtered list of params based on the content dict, and depending
of the screen resolution will take the informatino from the key 'p', 'l' or 's'
"""


def fn(config, w, h):
    v = None
    ar = w / h

    if ar < 1:
        v = [r for r in config['p'] if r['width'] <= w]
    elif ar > 4 / 3:
        v = [r for r in config['l'] if r['width'] <= w]
    else:
        v = [r for r in config['s'] if r['width'] <= w]

    return v


"""
Having the next helper, please implement a refactor to perform the API call using one method instead of rewriting the code
in the other methods.
"""

logger = logging.getLogger(__name__)


class Helper:
    DOMAIN = 'http://example.com'
    SEARCH_IMAGES_ENDPOINT = 'search/images'
    GET_IMAGE_ENDPOINT = 'image'
    DOWNLOAD_IMAGE_ENDPOINT = 'downloads/images'

    AUTHORIZATION_TOKEN = {
        'access_token': None,
        'token_type': None,
        'expires_in': 0,
        'refresh_token': None
    }

    def fetch_api(self, method, endpoint, **kwargs):
        token_type = self.AUTHORIZATION_TOKEN['token_type']
        access_token = self.AUTHORIZATION_TOKEN['access_token']
        headers = {'Authorization': f'{token_type} {access_token}'}

        url = f'{self.DOMAIN}/{endpoint}'
        response = requests.request(method, url, headers=headers, **kwargs)
        try:
            response.raise_for_status()
        except HTTPError as e:
            match e.response.status_code:
                case code if code >= 500:
                    logger.warning(f'There was {code} error server in {url}')
                case code if code >= 400:
                    logger.warning(f'There was {code} error server in {url}')
                case _:
                    logger.warning(f'There was a Error server in {url}')
            raise  # Optional, in case is desired to break the code
        except Timeout as e:
            logger.warning(f'This is a message in case of Timeout error. {e}')
            raise  # Optional, in case is desired to break the code
        except requests.exceptions.SSLError as e:
            logger.warning(f'This is a message in case of SSLError error. {e}')
            raise  # Optional, in case is desired to break the code
        else:
            return response.json() if response.text else {}

    def search_images(self, **kwargs):
        return self.fetch_api('get', self.SEARCH_IMAGES_ENDPOINT, **kwargs)

    def get_image(self, image_id, **kwargs):
        return self.fetch_api('get', f"{self.GET_IMAGE_ENDPOINT}/{image_id}", **kwargs)

    def download_image(self, image_id, **kwargs):
        return self.fetch_api('get', f"{self.DOWNLOAD_IMAGE_ENDPOINT}/{image_id}", **kwargs)
