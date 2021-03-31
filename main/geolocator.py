#------------------------------------------------------------------------------#
#                     Author     : Nicklas Sindlev Andersen                    #
#                     Website    : Nicklas.xyz                                 #
#                     Github     : github.com/NicklasXYZ                       #
#------------------------------------------------------------------------------#
#                                                                              #
#------------------------------------------------------------------------------#
#               Import packages from the python standard library               #
#------------------------------------------------------------------------------#
from datetime import timedelta
from datetime import datetime
import logging
import json
import time
from typing import (
    Union,
    List,
    Tuple,
    Dict,
    Any,
)
#------------------------------------------------------------------------------#
#                          Import local libraries/code                         #
#------------------------------------------------------------------------------#
#                                                                              #
#------------------------------------------------------------------------------#
#                      Import third-party libraries: Others                    #
#------------------------------------------------------------------------------#
import requests                        # pip install requests
import msgpack                         # pip install msgpack
import redis                           # pip install redis
import geohash_hilbert as ghh          # pip install geohash_hilbert
from shapely.geometry import (         # pip install shapely
    MultiPolygon,
    Polygon,
    Point,
)
from shapely.ops import unary_union
import numpy as np

#------------------------------------------------------------------------------#
#                         GLOBAL SETTINGS AND VARIABLES                        #
#------------------------------------------------------------------------------#
logging.basicConfig(level = logging.DEBUG)
EPOCH = datetime.fromtimestamp(0)


#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
class RedisClient:
    def __init__(
            self, host = "localhost", port = "6379", db = 1,
            timeout = 5, max_try = 5, wait_time = 0.001, ttl = 86400,
        ) -> None:
        """Initialize and set given class variables on class instantiation. 

        Args:
            host (str, optional): The service name or address of where Redis
                is running. Defaults to "localhost".
            port (str, optional): The port number of the address where Redis
                is running. Defaults to "6379".
            db (int, optional): The index of the Redis database that should
                be used. Defaults to 1.
            timeout (int, optional): An upper bound on the amount of time we
                are prepared to wait on the retrieval of a value before we
                return with an error. Defaults to 5.
            max_try (int, optional): An upper bound on the number of 
                connection retries. Defaults to 5.
            wait_time (float, optional): The time between repeated operations in
                Redis. Defaults to 0.001.
            ttl (int, optional): The amount of time after which a KV pair is
                deleted from Redis. Defaults to 86400 seconds, i.e., 24 hours.
        """
        # Redis store connection info
        self.host = host
        self.port = port
        self.db = db
        # Redis connection status
        self.server_down = True
        # The upper bound on number of re-tries
        self.max_try = max_try
        # The waiting time between re-tries, when processing data
        self.wait_time = wait_time
        # The expiry time of key-value pairs
        self.ttl = ttl
        # The timeout value used when trying to retrieve a value from Redis
        self.timeout = timeout
        # The backoff counter used to increase the waiting time 
        # between re-triees, when trying to re-connect to Redis
        self.backoff = 0
        # The client connection
        self.client = None
        # Check that the right args were given
        self.check_base_args()
        # Finally, connect to the Redis server
        self.connect()

    def check_base_args(self) -> None:
        """ Check and validate all class arguments on calss instantiation.

        Raises:
            TypeError: Given input is NOT of type 'float'.
            TypeError: Given input is NOT of type 'int'.
            TypeError: Given input is NOT of type 'float'.
            TypeError: Given input is NOT of type 'int'.
        """
        if not isinstance(self.timeout, float):
            error = f"ARG 'timeout' is of type {type(self.timeout)} " + \
                "but should be of type 'float'!"
            try:
                self.timeout = float(self.timeout)
            except:
                raise TypeError(error)
        if not isinstance(self.max_try, int):
            error = f"ARG 'max_try' is of type {type(self.max_try)} " + \
                "but should be of type 'int'!"
            try:
                self.max_try = int(self.max_try)
            except:
                raise TypeError(error)
        if not isinstance(self.wait_time, float):
            error = f"ARG 'wait_time' is of type {type(self.wait_time)} " + \
                "but should be of type 'float'!"
            try:
                self.wait_time = float(self.wait_time)
            except:
                raise TypeError(error)
        if not isinstance(self.ttl, int):
            error = f"ARG 'ttl' is of type {type(self.ttl)} " + \
                "but should be of type 'int'!"
            try:
                self.ttl = int(self.ttl)
            except:
                raise TypeError(error)

    def connect(self) -> bool:
        """ Connect to Redis and handle connection errors.

        Returns:
            bool: A boolean value that signals whether it was possible
                to connect to Redis.
        """
        connection_string = str(self.host) + ":" + str(self.port) + \
            "/" + str(self.db) 
        # Assume we are not able to connect.
        # This flag is set to 'True' if we are able to connect successfully.
        return_value = False
        while self.server_down:
            try:
                self._connect()
                self.server_down = False
                self.backoff = 0 # Reset the backoff value
                logging.debug(
                    f"Client connection to {connection_string} is up!"
                )
                return_value = True
                break
            except:
                self.server_down = True
                # After each connection retry increase the backoff value
                self.backoff += 1
                # Increase the waiting time before we try to reconnect to the
                # Redis KV store.
                sleep_time = 3 * self.backoff
                time.sleep(sleep_time)
                logging.debug(
                    f"Cannot connect to {connection_string} trying again " + \
                    f"in {sleep_time} seconds..."
                )
            if self.backoff == self.max_try:
                logging.debug(
                    f"No connection could be established to " + \
                    f"{connection_string} after {self.backoff} re-tries!"
                )
                break
        return return_value

    def reset_connection(self) -> bool:
        """ Try reconecting to Redis.

        Raises:
            redis.exceptions.ConnectionError: The reconnection attempt did 
                not work so raise a Redis 'ConnectionError'.

        Returns:
            bool: In case the reconnection attempt succeeded then return 'True'. 
        """
        # After 3 seconds try to re-connect...
        time.sleep(3)
        self.server_down = True
        is_connected = self.connect()
        if not is_connected:
            connection_string = str(self.host) + ":" + str(self.port) + \
                "/" + str(self.db) 
            logging.debug(
                f"Server is down. No connection could be established to " + \
                f"{connection_string}!"
            )
            raise redis.exceptions.ConnectionError
        else:
            return True

    def _connect(self) -> None:
        """ An internal method that is used to instantiate a Redis client 
        object and check if the connection to Redis is working. This is done
        by pinging the address and port where Redis is running.  
        """
        self.client = redis.Redis(
            host = self.host,
            port = self.port,
            db = self.db,
        )
        logging.debug(
            "Pinging the Redis server..."
        )
        while True:
            return_value = self.client.ping()
            if return_value == True:
                logging.debug(
                    f"The Redis server responded with {return_value}!"
                )
                break
            else:
                logging.debug(
                    "The Redis server did not respond..."
                )
            time.sleep(self.wait_time)


class OverpassClient:
    """A very simple client class for interfacing with the OSM Overpass API""" 

    def __init__(self, url: str = "http://localhost:12345/api/interpreter") -> None:
        """Initialize and set given class variables on class instantiation. 

        Args:
            url (str, optional): The url to which Overpass-specific HTTP requests
                should be sent to. Defaults to "http://localhost:12345/api/interpreter".
        """
        self.url = url

    def build_polygons(self, response: Dict[str, Any]) -> List[Polygon]:
        """Given a deserialized Overpass HTTP response, build new polygon objects that
        we can perform operations on using the 'shapely' python library. 

        Args:
            response (Dict[str, Any]): The raw polygon data retrieved from the Overpass
                API.

        Returns:
            List[Polygon]: Geometric object that we can query and operate on with the 
                'shapely' python library.
        """
        polygons = []
        for element in response["elements"]:
            if element["type"] == "relation":
                members = element["members"]
            else:
                members = [element]
            for member in members:
                try:
                    polygon = Polygon(
                        [[d["lat"], d["lon"]] for d in member["geometry"]]
                    )
                    polygons.append(polygon)
                except KeyError:
                    logging.warning(
                        "Function: '" + self.build_polygons.__name__ + "'. Problem: " + \
                        "Lat/Lon keys could not be accessed in the given dictionary!: "
                    )
        return list(unary_union(polygons)) 

    def retrieve_polygons(self, bbox: List[float]) -> Union[None, Dict[str, Any]]:
        """Retrieve all polygons in a given bounding box that can be categorized as a
        'building'.

        Args:
            bounding_box (List[float]): A list of coordinates:
                - A southern limit in decimal degrees (lowest latitude)
                - A western limit in decimal degrees (lowest longitude)
                - A northern limit in decimal degrees (highest latitude)
                - An eastern limit in decimal degrees (highest longitude)

        Returns:
            Union[None, Dict[str, Any]]: All polygons in the given bounding box,
                otherwise None.
        """
        # Build the Overpass query
        query = "[out:json][bbox:" + f"{','.join(map(str, bbox))}" + "][timeout:25];"
        query += \
            """
            (
                node['building'];
                way['building'];
                relation['building'];
            );
            out geom;
            """
        # Send a request to the Overpass API with the built query
        response = requests.get(
            self.url,
            params={"data": query}
        )
        # Check the return code. Only deserialize the data if we succeeded with the
        # request.
        if response.status_code == 200:
            return json.loads(response.content)
        else:
            logging.warning(
                "Function: '" +  self.retrieve_polygons.__name__ + "'. Problem: " + \
                "The Overpass API responded with status code: " + \
                str(response.status_code),
            )
            return None
    
    
class GeoHelper:
    """A helper class for working with 'Point' and 'Polygon' objects, along with
    Overpass for retrieving and Redis for caching these types of objects.
    """

    def __init__(self, precision: int = 5, bits_per_char: int = 6) -> None:
        """Initialize and set given class variables on class instantiation. 

        Args:
            precision (int, optional): Geohash precision. Defaults to 5.
            bits_per_char (int, optional): Geohash bits per character. Defaults to 6.
        """
        self.redis = RedisClient()
        self.overpass = OverpassClient()
        self.geohash_precision = precision
        self.geohash_bits_per_char = bits_per_char

    def encode_geoset(
        self,
        centroid: Tuple[float, ...],
        exterior: List[Tuple[float, ...]],
        ) -> Tuple[Any, ...]:
        """ Prepare the geospatial features to be inserted into a Redis sorted set,
        by transforming the data into the right format.

        Args:
            centroid (Tuple[float, ...]): The 'center' of a polygon that is used 
                as a representitive point.
            exterior (List[float]): The exterior boundry of a polygon as a sequence
                of points (latitude and longitude tuples).

        Returns:
            Tuple[Any, ...]: A 3-tuple consisting of the latitude and 
                longitude coordinates of the centroid of a polygon, along with the 
                exterior boundry of the polygon supplied as extra data.
        """
        coords_centroid = sum(list(centroid), ())
        coords_exterior = list(exterior)
        data = {
            "timestamp": str(datetime.utcnow()),
            "exterior": coords_exterior,
        }
        # Return 3-tuple: (lon, lat, extra data)
        # NOTE: This order should be used else queries will not work with Redis!
        return coords_centroid[1], coords_centroid[0], msgpack.dumps(data)

    def get_key(self, obj_type: str, geohash: str) -> str:
        """Construct an appropriate key for accessing a particular sorted set in 
        Redis.

        Args:
            obj_type (str): Object type/category used as a namespace.
            geohash (str): A geohash.

        Returns:
            str: A key to a Redis sorted set.
        """
        return f"{obj_type}:{geohash}"
    
    def enforce_ttl(self) -> None:
        """Enforce the set time-to-live (TTL) constraint. By removing elements 
        in Redis that have been cached for too long.
        """
        now = (datetime.utcnow() - EPOCH).total_seconds()
        keys = self.redis.client.zrangebyscore(
            name = "ttl",
            min = -np.inf,
            max = now,
        )
        for key in keys:
            return_code = self.redis.client.delete(key.decode("utf-8"))
            if return_code == 0:
                logging.warning(
                    "Function: '" +  self.enforce_ttl.__name__ + \
                    "'. Problem: The expired sorted set was not deleted!"
                )
    
    def set_ttl(self, key: str) -> None:
        """ Set the time-to-live (ttl) for a certain key.

        Args:
            key (str): The key to the Redis sorted set. 

        TODO: Handle retries!
        """
        # Create a separate sorted set to keep track of elements that 
        # should be removed in the future
        score = (datetime.utcnow() - EPOCH).total_seconds() + self.redis.ttl
        return_code = self.redis.client.zadd("ttl", {key: score})        
  
    def _add_geoset(self, key: str, geoset: Tuple[Any, ...]) -> int:
        """ Add geospatial data to a Redis sorted set by supplying a 'key' and
        the an encoded 'geoset'.

        Args:
            message (dict): The data that should be added to the Redis sorted set.
            key (str): The key to the Redis sorted set. 
        """
        start_time = datetime.utcnow() + timedelta(seconds = self.redis.timeout)
        geodata_added = False
        while True:
            if datetime.utcnow() - start_time > timedelta(seconds = self.redis.timeout):
                logging.warn(
                    f"Waited {self.redis.timeout} seconds. Data could not be added!"
                )
                return 0
            try:
                if geodata_added is False:
                    # Add the geospatial data to the sorted set
                    return_code = self.redis.client.geoadd(key, *geoset)
                    if return_code > 0:
                        geodata_added = True
                if geodata_added is True:
                    break
            except redis.exceptions.ConnectionError:
                # Try to fix the connection
                self.redis.reset_connection()
            time.sleep(self.redis.wait_time)
        # Set the ttl for the Redis sorted set with the given input key
        self.set_ttl(key = key)
        return 1 # Return status code 1

    def _get_geoset(
        self,
        key: str,
        lat: float,
        lon: float,
        bbox: List[float],
        k: int = 10, 
        radius: float = 750.,
        unit: str = "m",
        ) -> Union[None, List[Polygon]]:
        """Retrieve a set of geospatial features (a geoset) in a certain bounding box
        encoded by a geohash and near a given location.

        Args:
            key (str): The key used to specify the sorted set in Redis that should 
                be queried.
            lat (float): Latitude coordinate in decimal degrees.
            lon (float): Longitude coordinate in decimal degrees.
            bbox (Tuple[float,...]): A bounding box. 
            k (int, optional): The 'k' nearest geospatial features in the Redis sorted
                set to return. Defaults to 5.
            radius (float, optional): The radius in meters to look for polygon centroids.
                Defaults to 100.

        Returns:
            Union[None, List[Polygon]]: Return requested polygons if possible.
                Otherwise None.
        """
        # Evict data that have been been cached for too long 
        self.enforce_ttl()
        # Check if the sorted set that contains the 'geoset' exists in cache
        if self.redis.client.exists(key) == 0:
            # -> If the sorted set does not exist then retrieve the data from Overpass
            #    and cache the data by loading it into a Redis sorted set.
            logging.debug(
                "Function: '" +  self._get_geoset.__name__ + "'. Information: " + \
                "Requesting new information through the Overpass API!"
            )
            response = self.overpass.retrieve_polygons(bbox=bbox)
            if response is not None:
                polygons = self.overpass.build_polygons(response = response)
                for polygon in polygons:
                    geoset = self.encode_geoset(
                        centroid = polygon.centroid.coords,
                        exterior = polygon.exterior.coords,
                    )
                    self._add_geoset(key = key, geoset = geoset)
                return polygons
            else:
                logging.warning(
                    "Function: '" +  self._get_geoset.__name__ + "'. Problem: " + \
                    "The response was None! Could not retrieve geoset data!"
                )
                return None
        # -> Otherwise, retrieve the existing data from Redis
        else:
            logging.debug(
                "Function: '" +  self.get_geoset.__name__ + "'. Information: " + \
                "Reading cached data from Redis!"
            )
            geoset = self.redis.client.georadius(
                name = key,
                longitude = lon,
                latitude = lat,
                # Specify that the result and query is in input units 'unit'
                unit=unit,
                # Specify the lookup radius in input units 'unit'
                radius = radius,
                withdist=True,
                # Return the 'k' closest polygon centroids
                count=k,
                # Return polygon centroids in sorted order: Closest to farthest  
                sort="ASC",
            )
            return self.decode_geoset(geoset)
    
    def get_geoset(
        self,
        lat: float,
        lon: float,
        obj_type: str = "building",
        ) -> Union[None, List[Polygon]]:
        """Given a location in latitude and longitude coordinates, return 
        all polygons in vicinity. 

        Args:
            lat (float): The latitude coordinate of a location in decimal degrees.
            lon (float): The longitude coordinate of a location in decimal degrees.
            obj_type (str): The name used to construct a unique key for a Redis 
                sorted set. This name should describe the category of polygons we are
                indexing in Redis. Defaults to "building". 
                
                TODO: In the future we might want cache other types of polygons or be 
                more specific w.r.t. building type.

        Returns:
            Union[None, bool]: If the query was successful, then True is returned if 
                the location is inside a build. False if the location is not inside a 
                building and None if the query was not successful.
        """
        # Compute a geohash for the given location
        geohash = ghh.encode(
            lon, lat, # NOTE: Make sure first arg is lon, then lat! 
            precision = self.geohash_precision,
            bits_per_char = self.geohash_bits_per_char,
        )
        # Get the bounding box encoded by the geohash
        geohash_data = ghh.rectangle(geohash)
        geohash_bbox = geohash_data["bbox"]
        # Swap elements in bbox. We get elements (lon, lat), but we want (lat, lon)
        geohash_bbox = [
            geohash_bbox[1], geohash_bbox[0],
            geohash_bbox[3], geohash_bbox[2],
        ]
        # Contruct the key of the sorted set in Redis that we want to access
        key = self.get_key(obj_type = obj_type, geohash = geohash)
        polygons = self._get_geoset(
            key = key,
            lat = lat,
            lon = lon,
            bbox = geohash_bbox,
        )
        return polygons

    def is_inside(
        self,
        lat: float,
        lon: float,
        obj_type: str = "building",
        ) -> Union[None, bool]:
        """Given a location in latitude and longitude coordinates, check if the 
        location is contained inside a building. 

        Args:
            lat (float): The latitude coordinate of a location in decimal degrees.
            lon (float): The longitude coordinate of a location in decimal degrees.
            obj_type (str): The name used to construct a unique key for a Redis 
                sorted set. This name should describe the category of polygons we are
                indexing in Redis. Defaults to "building". 
                
                TODO: In the future we might want cache other types of polygons or be 
                more specific w.r.t. building type.

        Returns:
            Union[None, bool]: If the query was successful, then True is returned if 
                the location is inside a build. False if the location is not inside a 
                building and None if the query was not successful.
        """
        polygons = self.get_geoset(
            lat = lat,
            lon = lon,
            obj_type = obj_type,
        )
        if polygons is not None:
            point = Point(lat, lon)
            return self.point_in_polygon(point = point, polygon = polygons)
        else:
            return None 

    def decode_geoset(self, geoset: List[Any]) -> List[Polygon]:
        """Decode encoded geospatial data contained in the geoset. 

        Args:
            geoset (List[Any]): A list with encoded geospatial data.

        Returns:
            List[Polygon]: Geospatial features.
        """
        decoded_geoset = []
        for item in geoset:
            decoded_dict = msgpack.loads(item[0])
            decoded_geoset.append(
                Polygon(decoded_dict["exterior"])
            )
        return decoded_geoset

    def point_in_polygon(
        self,
        point: Point,
        polygon: Union[Polygon, MultiPolygon],
        ) -> bool:
        """Use the 'shapely' python library to check if a geometric point is contained
        inside a polygon.

        Args:
            point (Point): A geometric point represented by a latitude and longitude
                coordinate in decimal degrees. 
            polygon (Union[Polygon, MultiPolygon]): A sequnce of geometric points 
                each forming a polygon. 

        Returns:
            bool: True is returned if the point is inside a polygon. False if the 
                location is not inside a polygon.
        """
        for shape in polygon:
            if shape.contains(point):
                return True
        return False