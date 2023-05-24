import json
import mysql.connector
from mysql.connector import pooling

class Db:
    def __init__(self, config):
        try:
            self.connection_pool = pooling.MySQLConnectionPool(pool_name="pynative_pool",
                                                  pool_size=1,
                                                  pool_reset_session=True,
                                                  host=config["host"],
                                                  database=config["database"],
                                                  user=config["user"],
                                                  password=config["password"])
        except:
            print("error: No database pool created!")

    def get_voyage(self, id):
        voyage = self.exec("SELECT voyage_id, summary, year, DATE_FORMAT(`last_mutation`, \"%d-%m-%Y\") as last_mutation  FROM voyage WHERE voyage_id = " + id)
        voyage[0]["sub_voyage"] = self.get_subvoyages(id)
        return voyage[0]

    def get_global(self):
        voyages = self.exec("SELECT * FROM global_geo");
        return voyages;


    def get_subvoyages(self, id):
        ret_list = []
        subvoyages = self.exec("SELECT subvoyage_id, subvoyage_type, sub_dept_location, sub_dept_location_standardized, sub_dept_location_status, sub_dept_date_as_source, sub_dept_date_status, sub_arrival_location, sub_arrival_location_standardized,sub_arrival_location_status, sub_arrival_date_as_source, sub_arrival_date_status, sub_source, sub_vessel, sub_slaves FROM subvoyage WHERE voyage_id = " + id)
        for subvoyage in subvoyages:
            ret_list.append(self.build_subvoyage(subvoyage))
        return ret_list


    def build_subvoyage(self, subvoyage):
        ret_list = subvoyage
        ret_list["dep_coords"] = self.exec("SELECT DISTINCT location_latitude, location_longitude FROM standard_places WHERE location_name_standard = '" + subvoyage["sub_dept_location_standardized"] + "'")
        ret_list["arr_coords"] = self.exec("SELECT DISTINCT location_latitude, location_longitude FROM standard_places WHERE location_name_standard = '" + subvoyage["sub_arrival_location_standardized"] + "'")
        ret_list["vessel"] = self.exec("SELECT transport_name, transport_type, transport_capacity, transport_source FROM vessel WHERE vessel_id = " + str(subvoyage["sub_vessel"]))
        ret_list["cargo"] = self.exec("SELECT cargo_commodity, cargo_unit, cargo_quantity, cargo_value, cargo_source FROM cargo WHERE subvoyage_subvoyage_id = " + str(subvoyage["subvoyage_id"]))
        ret_list["slaves"] = self.build_slave_info(ret_list["sub_slaves"])
        return ret_list

    def build_slave_info(self, id):
        ret_list = self.exec("SELECT slaves_id, slaves_total, slaves_total_status, slaves_mortality, slaves_type, slaves_notes, slaves_source FROM slaves WHERE slaves_id =" + str(id))
        if len(ret_list):
            ret_list[0]["groups"] = self.exec("SELECT gr_sex, gr_age_group, gr_ethnicity, gr_physical_state, gr_quantity, gr_notes FROM slaves_group WHERE slaves_id = " + str(id))
            return ret_list[0]
        else:
            return ret_list



    def exec(self, sql):
        # con = self.conn = mysql.connector.connect(
        #     host=self.host,
        #     user=self.user,
        #     password=self.password,
        #     database=self.database,
        # )
        connection_object = self.connection_pool.get_connection()
        json_data=[]
        if connection_object.is_connected():
            cursor = connection_object.cursor()
            cursor.execute(sql)
            rv = cursor.fetchall()
            row_headers=[x[0] for x in cursor.description]
            for result in rv:
                json_data.append(dict(zip(row_headers,result)))
            cursor.close()
            connection_object.close()
        return json_data
