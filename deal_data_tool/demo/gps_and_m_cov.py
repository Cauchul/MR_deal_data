from pyproj import Transformer

tf_for_gps_to_utm = Transformer.from_crs(4326, 32750)
tf_for_utm_to_gps = Transformer.from_crs(32750, 4326)


def transform_gps_to_utm(in_longitude, in_latitude):
    utm_x, utm_y = tf_for_gps_to_utm.transform(in_latitude, in_longitude)
    # utm_x, utm_y = tf_for_gps_to_utm.transform(in_longitude, in_latitude)
    return utm_x, utm_y


def transform_utm_to_gps(in_x, in_y):
    tmp_longitude, tmp_latitude = tf_for_utm_to_gps.transform(in_x, in_y)
    return tmp_longitude, tmp_latitude


if __name__ == '__main__':
    longitude, latitude = 116.30297229, 39.93413488
    x, y = transform_gps_to_utm(longitude, latitude)
    print(x, y)
    longitude2, latitude2 = transform_utm_to_gps(x, y)
    print(f'longitude, latitude {longitude}, {latitude}, longitude2, latitude2 {longitude2}, {latitude2}')
