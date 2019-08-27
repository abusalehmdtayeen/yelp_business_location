import csv
import scrapy
import re

file_path = "/home/tayeen/Yelp/city_data/"

def read_csv(file_name, fieldnames):
	csv_data = []
	print "Reading csv file...."
	with open(file_name+'.csv', 'r') as csvfile:
		csvreader = csv.DictReader(csvfile)
		next(csvreader) # This skips the first row of the CSV file.
		for row in csvreader:
			record = {}
			for field in fieldnames:
				record[field] = row[field]
			csv_data.append(record)

	#print csv_data
	return csv_data
#-------------------------------

class ZipSpider(scrapy.Spider):
 	name = "zipinfo"

 	state_records = read_csv(file_path+"AZ_postal_codes", ["Zip Code", "Place Name", "State", "State Abbreviation", "County", "Latitude", "Longitude"])
 	start_urls = ["http://www.city-data.com/zips/"+str(record['Zip Code'].strip())+".html" for record in state_records]
 	#with open(file_path+"AZ_counties.txt", "rt") as f:
 		#start_urls = ["http://www.city-data.com/county/"+county.strip()+"_County-AZ.html" for county in f.readlines()]
 	#start_urls = ['http://www.city-data.com/zips/85255.html'] #85087, 85003, 85009, 85206
 	    
 	def start_requests(self):
 		for indx, url in enumerate(self.start_urls):
 			yield scrapy.Request(url=url, callback=self.parse, meta={'input_url': url, 'url_index': indx})
 

 	def parse(self, response):
 		in_url = response.meta['input_url']
 		url_indx = response.meta['url_index'] 

 		zip_code = in_url[in_url.rfind("/")+1: len(in_url)-5]
 		print "=================================="
 		print zip_code
 		print "==================================="

 		location_names = response.css("div.alert:nth-child(1) a::text").extract()
 		location_urls = response.css("div.alert:nth-child(1) a::attr(href)").extract()
 		location_proximity = response.css("div.alert:nth-child(1) small::text").extract()
 		#print location_names
		#print location_urls
 		#print location_proximity
 		locations = []
 		county_obj = None
 		state = None
 		for indx, loc in enumerate(location_names):
 			state = loc.split(",")[1].strip()
 			loc_name = loc.split(",")[0].strip()
 			if "County" in loc_name:
 				county_obj = { 'name': loc_name[0:loc_name.rfind("County")].strip(), 'url': location_urls[indx] }
 			else:
 				try:
 					proximity_txt = location_proximity[indx]
 					proximity_val = float(proximity_txt[proximity_txt.find("(")+1: proximity_txt.rfind("%")].strip())
 					location_obj = { 'name': loc_name, 'proximity': proximity_val, 'url': location_urls[indx] } 
 				except IndexError:
 					location_obj = { 'name': loc_name, 'url': location_urls[indx] }
 								
 				locations.append(location_obj)

 		population_info = self.build_population_obj(response) #population
 		housing_info = self.build_housing_obj(response) #housing
 		#-------------Mar. 2016 cost of living index----------------- 
 		cost_of_living = float(response.xpath("//*[contains(text(), 'Mar. 2016 cost of living index in zip code')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first())
 		#print "Cost_of_living: %f"%cost_of_living
 		#--------------------------------------------------------------		
 		density_info = self.build_density_obj(response) #density
 		taxes_info = self.build_taxes_obj(response) #taxes_paid
 		#--------------population by sex-------------
 		males_text = response.xpath("//*[contains(text(), 'Males:')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		males_num = int((males_text.strip()).replace(",", ""))
 		females_text = response.xpath("//*[contains(text(), 'Females:')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		females_num = int((females_text.strip()).replace(",", ""))
 		#print ("Males: %d, Females: %d")%(males_num, females_num)
 		#--------------For population 25 years and over---------------
 		population_25_over_obj = self.build_population_25_over_obj(response)
 		#--------------For population 15 years and over---------------
 		population_15_over_obj = self.build_population_15_over_obj(response)
 		#--------------Races-------------------------
 		races_info = self.build_races_obj(response) #races		
 		household_info = self.build_household_obj(response)

 		taxes_extra = self.build_taxes_extra_obj(response)

 		#----------Nearest zip codes--------------
 		nearest_zips = response.xpath("//*[contains(text(), 'Nearest zip codes')]/following-sibling::a[contains(@href,'zips')]/text()").extract()
 		#Mean price
 		mean_price_obj = self.build_mean_price_obj(response)

 		#-----------Means of transportation to work in zip------------
 		transportation_obj = self.build_transportation_obj(response)       
 		#-----------Travel time to work-----------
 		travel_time_obj = self.build_travel_time_obj(response)
 		#-----------Unemployment--------------------	
 		unemployment_text = response.xpath("//*[contains(text(), 'Unemployment')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		unemployment = float(unemployment_text[unemployment_text.find(":")+1: unemployment_text.rfind("%")].strip())
 		#print "Unemployment: %f"%unemployment

 		#----Percentage of zip code residents living and working in this county------
 		zip_residents_working_text = response.xpath("//*[contains(text(), 'Percentage of zip code residents living and working in this county')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		zip_residents_working = float(zip_residents_working_text[: zip_residents_working_text.rfind("%")].strip())
 		#print "zip_residents_working: %f"%zip_residents_working

 		#---------Most commonly used house heating fuel--------
 		heating_fuel_obj = self.build_heating_fuel_obj(response)

 		#---------Private vs. public school enrollment---------
 		school_enrollment_obj = self.build_school_enrollment_obj(response)

 		#---------Occupation by median earnings in the past 12 months-------
 		occupation_obj = self.build_occupation_obj(response)

 		#--------Fatal accident statistics in 2014-----------
 		accident_obj = self.build_accident_obj(response)

 		#------Zip code household income distribution in 2016-------
 		household_income_obj = self.build_household_income_obj(response)
 
 		#------Notable locations in this zip code-------
 		notable_locations_obj = self.build_notable_locations_obj(response)
 		
 		yield {'zip_code': int(zip_code), 'state': state, 'county': county_obj, 'nearest_locations': locations, 'nearest_zips': nearest_zips,'cost_of_living': cost_of_living, 'population_info': {'total': population_info, 'males': males_num, 'females': females_num, '25_years_and_over': population_25_over_obj, '15_years_and_over': population_15_over_obj }, 'density_info': density_info, 'housing_info': { 'detail': housing_info, 'mean_price': mean_price_obj }, 'races': races_info, 'taxes': {'paid': taxes_info, 'extra': taxes_extra} , 'household_info': {'detail': household_info, 'income': household_income_obj}, 'transportation': transportation_obj, 'travel_time': travel_time_obj, 'unemployment': unemployment, 'residents_working_in_county': zip_residents_working, 'notable_locations': notable_locations_obj, 'occupation': occupation_obj, 'accidents': accident_obj, 'school_enrollment': school_enrollment_obj, 'heating_fuel':heating_fuel_obj} 
 		
    
 	#------------------------------------
 	def build_population_obj(self, response):
 		total_population_2016_text = response.xpath("//*[contains(text(), 'Estimated zip code population in 2016')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		total_population_2016 = int((total_population_2016_text.strip()).replace(",", ""))
 		#print total_population_2016
 		
 		total_population_2010_text = response.xpath("//*[contains(text(), 'Zip code population in 2010')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		total_population_2010 = int((total_population_2010_text.strip()).replace(",", ""))
 		#print total_population_2010

 		total_population_2000_text = response.xpath("//*[contains(text(), 'Zip code population in 2000')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		total_population_2000 = int((total_population_2000_text.strip()).replace(",", ""))
 		#print total_population_2000

 		#total_population_text = total_population_text.replace(" ", "_")		#re.sub("\s+", " ", total_population_key).strip() 
 		
 		population_obj = { '2016': total_population_2016, '2010': total_population_2010, '2000': total_population_2000 }
        
 		#print population_obj
 		return population_obj
 	#-------------------------------------
 	def build_housing_obj(self, response):
 		total_houses_text = response.xpath("//*[contains(text(), 'Houses and condos')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		if total_houses_text is not None:
 			total_houses = int((total_houses_text.strip()).replace(",", ""))
 		else:
 			total_houses = None
 		#print total_houses	
 		
 		total_apts_text = response.xpath("//*[contains(text(), 'Renter-occupied apartments')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		if total_apts_text is not None:
 			total_apts = int((total_apts_text.strip()).replace(",", ""))
 		else:
			total_apts = None
 		#print total_apts

 		# % of renters in the zipcode
 		zip_renters_text = response.xpath("//*[contains(text(), '% of renters')]/ancestor::div[contains(@class,'hgraph')]/table/tr[1]/td[2]/text()").extract_first()
 		#zip_renters_text = response.css("div.hgraph:nth-child(13)").xpath(".//table/tr[1]/td[2]/text()").extract_first()
 		if zip_renters_text is not None:
 			zip_renters = float(zip_renters_text[0:zip_renters_text.rfind("%")].strip())
 		else:
 			zip_renters = None
 		#print zip_renters		
 		# % of renters in state
 		state_renters_text = response.xpath("//*[contains(text(), '% of renters')]/ancestor::div[contains(@class,'hgraph')]/table/tr[2]/td[2]/text()").extract_first()
 		#state_renters_text = response.css("div.hgraph:nth-child(13)").xpath(".//table/tr[2]/td[2]/text()").extract_first()
 		if state_renters_text is not None: 
 			state_renters = float(state_renters_text[0:state_renters_text.rfind("%")].strip())
 		else:
 			state_renters = None		
 		#print state_renters

 		housing_obj = {'houses_condos': total_houses, 'renter_apts': total_apts, 
 					   'zipcode_renters': zip_renters, 'state_renters': state_renters  
                      }

 		#print housing_obj
 		return housing_obj
 	#-------------------------------------
 	def build_density_obj(self, response):
 		land_area_text = response.xpath("//*[contains(text(), 'Land area')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		water_area_text = response.xpath("//*[contains(text(), 'Water area')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		density_text = response.xpath("//*[contains(text(), 'Population density')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()

 		land_area = float((land_area_text[0:land_area_text.find("sq")].strip()).replace(",", "")) #Land area in square mile
 		water_area = float((water_area_text[0:water_area_text.find("sq")].strip()).replace(",", "")) 	#Water area in square mile
 		population_density = float((density_text[0:density_text.find("people")].strip()).replace(",", "")) #people per square mile in average
 		
 		density_obj = {'land_area': land_area, 'water_area': water_area, 'population_density': population_density }
 					  
 		#print density_obj
 		return density_obj
 	#------------------------------------
 	def build_taxes_obj(self, response):
 		#Real estate property taxes paid for housing units in 2016
 		zip_taxes_text = response.xpath("//*[contains(text(), 'Real estate property taxes paid for housing units in 2016')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[1]/td[2]/text()").extract_first()	
 		#zip_taxes_text = response.css("div.hgraph:nth-child(35)").xpath(".//table/tr[1]/td[2]/text()").extract_first()
 		if zip_taxes_text is not None:
 			zip_taxes_per = float(zip_taxes_text.split("%")[0].strip())
 			zip_taxes_temp_num = zip_taxes_text.split("%")[1]
 			zip_taxes_num = int(zip_taxes_temp_num[zip_taxes_temp_num.find("$")+1:zip_taxes_temp_num.rfind(")")].strip().replace(",",""))
 		else:
 			zip_taxes_per = None
 			zip_taxes_num = None

 		state_taxes_text = response.xpath("//*[contains(text(), 'Real estate property taxes paid for housing units in 2016')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[2]/td[2]/text()").extract_first()
 		#state_taxes_text = response.css("div.hgraph:nth-child(35)").xpath(".//table/tr[2]/td[2]/text()").extract_first()
 		if state_taxes_text is not None:
 			state_taxes_per = float(state_taxes_text.split("%")[0].strip())
 			state_taxes_temp_num = state_taxes_text.split("%")[1] 
 			state_taxes_num = int(state_taxes_temp_num[state_taxes_temp_num.find("$")+1: state_taxes_temp_num.rfind(")")].strip().replace(",",""))
 		else:
 			state_taxes_per = None
 			state_taxes_num = None

 		#Median real estate property taxes paid for housing units with mortgages in 2016
 		median_tax_w_mortg_txt = response.xpath("//*[contains(text(), 'Median real estate property taxes paid for housing units with mortgages in 2016')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		if median_tax_w_mortg_txt is not None:
 			median_tax_w_mortg_tmp = median_tax_w_mortg_txt.split("(")
 			median_tax_w_mortg_num = int(median_tax_w_mortg_tmp[0][median_tax_w_mortg_tmp[0].find("$")+1:].strip().replace(",",""))
 			median_tax_w_mortg_per = float(median_tax_w_mortg_tmp[1][0: median_tax_w_mortg_tmp[1].rfind("%")].strip())
 		else:
 			median_tax_w_mortg_num = None
 			median_tax_w_mortg_per = None

 		#Median real estate property taxes paid for housing units with no mortgage in 2016
 		median_tax_wo_mortg_txt = response.xpath("//*[contains(text(), 'Median real estate property taxes paid for housing units with no mortgage in 2016')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		if median_tax_wo_mortg_txt is not None:
 			median_tax_wo_mortg_tmp = median_tax_wo_mortg_txt.split("(")
 			median_tax_wo_mortg_num = int(median_tax_wo_mortg_tmp[0][median_tax_wo_mortg_tmp[0].find("$")+1:].strip().replace(",",""))
 			median_tax_wo_mortg_per = float(median_tax_wo_mortg_tmp[1][0: median_tax_wo_mortg_tmp[1].rfind("%")].strip())
 		else:
 			median_tax_wo_mortg_num = None
 			median_tax_wo_mortg_per = None

 		taxes_obj = {'zip_housing_num': zip_taxes_num, 'zip_housing_per': zip_taxes_per,
 					 'state_housing_num': state_taxes_num, 'state_housing_per': state_taxes_per,
 					 'housing_w_mort_num': median_tax_w_mortg_num, 'housing_w_mort_per': median_tax_w_mortg_per,
 					 'housing_wo_mort_num': median_tax_wo_mortg_num, 'housing_wo_mort_per': median_tax_wo_mortg_per
 					}
 					  
 		#print taxes_obj
 		return taxes_obj

 	#-------------------------------------
 	def build_races_obj(self, response):
 		races_list = response.xpath("//*[contains(text(), 'Races in zip code')]/../following-sibling::li[not(contains(@class,'text-center'))]/ul/li/text()").extract()
 		#races_list = response.css("div.col-md-8:nth-child(1) > ul:nth-child(1) > li:nth-child(3) > ul:nth-child(1) li::text").extract()
 		races_num_text = response.xpath("//*[contains(text(), 'Races in zip code')]/../following-sibling::li[not(contains(@class,'text-center'))]/ul/li/span/text()").extract()
 		#races_num_text = response.css("div.col-md-8:nth-child(1) > ul:nth-child(1) > li:nth-child(3) > ul:nth-child(1) li span::text").extract() 
 		#--------races--------
 		races_dict = {}
 		for indx, race_name in enumerate(races_list):
 			race_num = races_num_text[indx]
 			race_value = int(race_num.strip().replace(",", ""))
 			#print ("%s, %s")%(race_name, race_num)
 			race_key = race_name[0:race_name.rfind("population")-1].lower().replace(" ","_")
 			races_dict[race_key] = race_value
		  
 		#print races_dict
 		return races_dict
 	#-------------------------------------
 	def build_household_obj(self, response):
 		#Estimated median house or condo value in 2016
 		zip_house_condo_value_text = response.xpath("//*[contains(text(), 'Estimated median house/condo value in 2016')]/following-sibling::node()/tr[1]/td[2]/text()").extract_first() 
 		zip_house_condo_value = int(zip_house_condo_value_text[1:].strip().replace(",",""))
 		state_house_condo_value_text = response.xpath("//*[contains(text(), 'Estimated median house/condo value in 2016')]/following-sibling::node()/tr[2]/td[2]/text()").extract_first()
 		state_house_condo_value = int(state_house_condo_value_text[1:].strip().replace(",",""))
 		
 		#Median resident age
 		zip_median_resident_age_text = response.xpath("//*[contains(text(), 'Median resident age')]/following-sibling::node()/tr[1]/td[2]/text()").extract_first() 
 		zip_median_resident_age = float(zip_median_resident_age_text[:zip_median_resident_age_text.rfind("years")].strip())
 		state_median_resident_age_text = response.xpath("//*[contains(text(), 'Median resident age')]/following-sibling::node()/tr[2]/td[2]/text()").extract_first()
 		state_median_resident_age = float(state_median_resident_age_text[:state_median_resident_age_text.rfind("years")].strip())
 		
 		#Average household size
 		zip_average_household_size_text = response.xpath("//*[contains(text(), 'Average household size')]/following-sibling::node()/tr[1]/td[2]/text()").extract_first() 
 		zip_average_household_size = float(zip_average_household_size_text[:zip_average_household_size_text.rfind("people")].strip())
 		state_average_household_size_text = response.xpath("//*[contains(text(), 'Average household size')]/following-sibling::node()/tr[2]/td[2]/text()").extract_first()
 		state_average_household_size = float(state_average_household_size_text[:state_average_household_size_text.rfind("people")].strip())
 		
 		
 		#Estimated median household income in 2016
 		zip_household_income_text = response.xpath("//*[contains(text(), 'Estimated median household income in 2016')]/../following-sibling::table/tr[1]/td[2]/text()").extract_first()		
 		#zip_household_income_text = response.css("div.row:nth-child(3) > div:nth-child(80)").xpath(".//table/tr[1]/td[2]/text()").extract_first()
 		zip_household_income = int(zip_household_income_text[1:].strip().replace(",",""))

 		state_household_income_text = response.xpath("//*[contains(text(), 'Estimated median household income in 2016')]/../following-sibling::table/tr[2]/td[2]/text()").extract_first()
 		#state_household_income_text = response.css("div.row:nth-child(3) > div:nth-child(80)").xpath(".//table/tr[2]/td[2]/text()").extract_first()
 		state_household_income = int(state_household_income_text[1:].strip().replace(",",""))

 		#Percentage of family households
 		zip_family_households_text = response.xpath("//*[contains(text(), 'Percentage of family households')]/following-sibling::table/tr[1]/td[2]/text()").extract_first()
 		#zip_family_households_text = response.css("div.hgraph:nth-child(93)").xpath(".//table/tr[1]/td[2]/text()").extract_first()
 		zip_family_households = float(zip_family_households_text.strip().replace("%",""))

 		state_family_households_text = response.xpath("//*[contains(text(), 'Percentage of family households')]/following-sibling::table/tr[2]/td[2]/text()").extract_first()
 		#state_family_households_text = response.css("div.hgraph:nth-child(93)").xpath(".//table/tr[2]/td[2]/text()").extract_first()
 		state_family_households = float(state_family_households_text.strip().replace("%",""))

 		#Percentage of households with unmarried partners
 		zip_unmarried_partner_households_text = response.xpath("//*[contains(text(), 'Percentage of households with unmarried partners')]/following-sibling::table/tr[1]/td[2]/text()").extract_first()
 		#zip_unmarried_partner_households_text = response.css("div.hgraph:nth-child(94)").xpath(".//table/tr[1]/td[2]/text()").extract_first()
 		zip_unmarried_partner_households = float(zip_unmarried_partner_households_text.strip().replace("%",""))

 		state_unmarried_partner_households_text = response.xpath("//*[contains(text(), 'Percentage of households with unmarried partners')]/following-sibling::table/tr[2]/td[2]/text()").extract_first()
 		#state_unmarried_partner_households_text = response.css("div.hgraph:nth-child(94)").xpath(".//table/tr[2]/td[2]/text()").extract_first()
 		state_unmarried_partner_households = float(state_unmarried_partner_households_text.strip().replace("%",""))

 		#Household received Food Stamps/SNAP in the past 12 months
 		household_w_food_stamps_txt = response.xpath("//*[contains(text(), 'Household received Food Stamps/SNAP in the past 12 months')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		household_w_food_stamps = int(household_w_food_stamps_txt.replace(",", ""))

 		#Household did not receive Food Stamps/SNAP in the past 12 months
 		household_wo_food_stamps_txt = response.xpath("//*[contains(text(), 'Household did not receive Food Stamps/SNAP in the past 12 months')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		household_wo_food_stamps = int(household_wo_food_stamps_txt.replace(",", ""))

 		#Women who had a birth in the past 12 months
 		woman_birth_txt = response.xpath("//*[contains(text(), 'Women who had a birth in the past 12 months')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		woman_birth = int(woman_birth_txt[:woman_birth_txt.rfind("(")].strip().replace(",", ""))

 		#Women who did not have a birth in the past 12 months
 		woman_not_birth_txt = response.xpath("//*[contains(text(), 'Women who did not have a birth in the past 12 months')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		woman_not_birth = int(woman_not_birth_txt[:woman_not_birth_txt.rfind("(")].strip().replace(",", ""))

 		#Housing units in zip code
 		housing_units_txt = response.xpath("//*[contains(text(), 'Housing units in zip code')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		housing_units = int(housing_units_txt[:housing_units_txt.rfind("(")].strip().replace(",", ""))

 		#Houses without a mortgage
 		houses_wo_mortgage_txt = response.xpath("//*[contains(text(), 'Houses without a mortgage')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		houses_wo_mortgage = int(houses_wo_mortgage_txt.strip().replace(",", ""))

 		#Median monthly owner costs for units with a mortgage
 		cost_w_mortgage_txt = response.xpath("//*[contains(text(), 'Median monthly owner costs for units with a mortgage')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		cost_w_mortgage = int(cost_w_mortgage_txt[2:].strip().replace(",", ""))

 		#Median monthly owner costs for units without a mortgage
 		cost_wo_mortgage_txt = response.xpath("//*[contains(text(), 'Median monthly owner costs for units without a mortgage')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		cost_wo_mortgage = int(cost_wo_mortgage_txt[2:].strip().replace(",", ""))

 		#Residents with income below the poverty level in 2016
 		zip_residents_below_poverty_txt = response.xpath("//*[contains(text(), 'Residents with income below the poverty level in 2016')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[1]/td[2]/text()").extract_first()
 		#zip_residents_below_poverty_txt = response.css("div.hgraph:nth-child(115)").xpath(".//table/tr[1]/td[2]/text()").extract_first()
 		zip_residents_below_poverty = float(zip_residents_below_poverty_txt.strip().replace("%", ""))

 		state_residents_below_poverty_txt = response.xpath("//*[contains(text(), 'Residents with income below the poverty level in 2016')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[2]/td[2]/text()").extract_first()		
 		#state_residents_below_poverty_txt = response.css("div.hgraph:nth-child(115)").xpath(".//table/tr[2]/td[2]/text()").extract_first()
 		state_residents_below_poverty = float(state_residents_below_poverty_txt.strip().replace("%", ""))

 		#Residents with income below 50% of the poverty level in 2016
 		zip_residents_below_50_poverty_txt = response.xpath("//*[contains(text(), 'Residents with income below 50% of the poverty level in 2016')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[1]/td[2]/text()").extract_first()
 		#zip_residents_below_50_poverty_txt = response.css("div.hgraph:nth-child(118)").xpath(".//table/tr[1]/td[2]/text()").extract_first()
 		zip_residents_below_50_poverty = float(zip_residents_below_50_poverty_txt.strip().replace("%", ""))

 		state_residents_below_50_poverty_txt = response.xpath("//*[contains(text(), 'Residents with income below 50% of the poverty level in 2016')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[2]/td[2]/text()").extract_first()
 		#state_residents_below_50_poverty_txt = response.css("div.hgraph:nth-child(118)").xpath(".//table/tr[2]/td[2]/text()").extract_first()
 		state_residents_below_50_poverty = float(state_residents_below_50_poverty_txt.strip().replace("%", ""))

 		#Median number of rooms in houses and condos
 		zip_rooms_houses_condos_txt = response.xpath("//*[contains(text(), 'Median number of rooms in houses and condos')]/following-sibling::table/tr[1]/td[2]/text()").extract_first()
 		#zip_rooms_houses_condos_txt = response.css("div.hgraph:nth-child(119)").xpath(".//table/tr[1]/td[2]/text()").extract_first()
 		zip_rooms_houses_condos = float(zip_rooms_houses_condos_txt.strip())

 		state_rooms_houses_condos_txt = response.xpath("//*[contains(text(), 'Median number of rooms in houses and condos')]/following-sibling::table/tr[2]/td[2]/text()").extract_first()
 		#state_rooms_houses_condos_txt = response.css("div.hgraph:nth-child(119)").xpath(".//table/tr[2]/td[2]/text()").extract_first()
 		state_rooms_houses_condos = float(state_rooms_houses_condos_txt.strip())

 		#Median number of rooms in apartments
 		zip_rooms_apts_txt = response.xpath("//*[contains(text(), 'Median number of rooms in apartments')]/following-sibling::table/tr[1]/td[2]/text()").extract_first()
 		#zip_rooms_apts_txt = response.css("div.hgraph:nth-child(120)").xpath(".//table/tr[1]/td[2]/text()").extract_first()
 		zip_rooms_apts = float(zip_rooms_apts_txt.strip())

 		state_rooms_apts_txt = response.xpath("//*[contains(text(), 'Median number of rooms in apartments')]/following-sibling::table/tr[2]/td[2]/text()").extract_first()
 		#state_rooms_apts_txt = response.css("div.hgraph:nth-child(120)").xpath(".//table/tr[2]/td[2]/text()").extract_first()
 		state_rooms_apts = float(state_rooms_apts_txt.strip())


 		household_obj = {'avg_household_size': {'zip': zip_average_household_size, 'state': state_average_household_size}, \
 						 'median_resident_age': {'zip': zip_median_resident_age, 'state': state_median_resident_age}, \
 						 'median_house_condo_value': {'zip': zip_house_condo_value, 'state': state_house_condo_value}, \
 						 'household_income': {'zip': zip_household_income, 'state': state_household_income}, \
 						 'family_households': {'zip': zip_family_households, 'state': state_family_households}, \
 						 'unmarried_partner_households': {'zip': zip_unmarried_partner_households, 'state': state_unmarried_partner_households}, \
 						 'household_food_stamps': {'received': household_w_food_stamps, 'not_received': household_wo_food_stamps} , \
 						 'median_monthly_owner_cost' : {'with_mortgage': cost_w_mortgage, 'without_mortgage': cost_wo_mortgage} , \
 						 'woman_birth': {'had': woman_birth, 'had not': woman_not_birth}, \
 						 'housing_units': housing_units ,\
 						 'houses_wo_mortgage': houses_wo_mortgage, \
 						 'residents_below_poverty' : {'zip': zip_residents_below_poverty, 'state': state_residents_below_poverty} , \
 						 'residents_below_50_poverty' : {'zip': zip_residents_below_50_poverty, 'state': state_residents_below_50_poverty} ,\
 						 'rooms_houses_condos' : {'zip': zip_rooms_houses_condos, 'state': state_rooms_houses_condos} ,\
 						 'rooms_apts' : {'zip': zip_rooms_apts, 'state': state_rooms_apts}
 						 }
				  
 		#print household_obj
 		return household_obj
 	#---------------------------------------
 	def build_mean_price_obj(self, response):
 		#--------Mean price in 2016--------
 		mean_prices_titles = response.xpath("//*[contains(text(), 'Mean price in 2016')]/following-sibling::node()/b/text()").extract() 
 		zip_mean_prices = response.xpath("//*[contains(text(), 'Mean price in 2016')]/following-sibling::node()/div[contains(@class,'hgraph')]/table/tr[1]/td[2]/text()").extract() 
 		state_mean_prices = response.xpath("//*[contains(text(), 'Mean price in 2016')]/following-sibling::node()/div[contains(@class,'hgraph')]/table/tr[2]/td[2]/text()").extract()  		

 		#Detached houses, Townhouses or other attached units, Mobile homes, Occupied boats, RVs, vans, etc
 		mean_prices_dict = {}
 		for indx, mean_price in enumerate(zip_mean_prices):
 			zip_mean_text = zip_mean_prices[indx]
 			state_mean_text =  state_mean_prices[indx]
 			zip_mean = int((zip_mean_text[1:len(zip_mean_text)].strip()).replace(",", ""))
 			state_mean = int((state_mean_text[1:len(state_mean_text)].strip()).replace(",", ""))
 			mean_price_key = mean_prices_titles[indx].lower().replace(" ","_").replace(",", "").replace(".","").replace(":","")
 			mean_prices_dict[mean_price_key] = {'zip': zip_mean, 'state': state_mean}

 		#print mean_prices_dict
 		return mean_prices_dict

 	#------------------------------------
 	def build_population_25_over_obj(self, response):
 		high_school_higher_text = response.xpath("//*[contains(text(), 'High school or higher')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		if high_school_higher_text is not None:
 			high_school_higher = float((high_school_higher_text.strip()).replace("%", ""))
 		else:
 			high_school_higher = None
 		#print high_school_higher
 		
 		bachelor_higher_text = response.xpath("//*[contains(text(), \"Bachelor's degree or higher\")]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		if bachelor_higher_text is not None:
 			bachelor_higher = float((bachelor_higher_text.strip()).replace("%", ""))
 		else:
 			bachelor_higher = None		
 		#print bachelor_higher

 		graduate_professional_text = response.xpath("//*[contains(text(), 'Graduate or professional degree')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		if graduate_professional_text is not None:
 			graduate_professional = float((graduate_professional_text.strip()).replace("%", ""))
 		else:
 			graduate_professional = None		
 		#print graduate_professional

 		unemployed_text = response.xpath("//*[contains(text(), 'Unemployed')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		if unemployed_text is not None:
 			unemployed = float((unemployed_text.strip()).replace("%", ""))
 		else:
 			unemployed = None
 		#print unemployed

 		mean_travel_time_text = response.xpath("//*[contains(text(), 'Mean travel time to work (commute)')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		if mean_travel_time_text is not None:
 			mean_travel_time = float((mean_travel_time_text[0:mean_travel_time_text.rfind("minutes")].strip()).replace("%", ""))
 		else:
 			mean_travel_time = None
 		#print mean_travel_time
 		#total_population_text = total_population_text.replace(" ", "_")		#re.sub("\s+", " ", total_population_key).strip() 
 		
 		population_25_over_obj = { 'high_school_higher': high_school_higher, 'bachelor_higher': bachelor_higher, 'graduate_professional': graduate_professional, 'unemployed': unemployed, 'mean_travel_time_work': mean_travel_time}
        
 		#print population_25_over_obj
 		return population_25_over_obj
 	#-------------------------------------
 	def build_population_15_over_obj(self, response):
 		never_married_text = response.xpath("//*[contains(text(), 'Never married')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		if never_married_text is not None:
 			never_married = float((never_married_text.strip()).replace("%", ""))
 		else:
 			never_married = None
 		
 		now_married_text = response.xpath("//*[contains(text(), 'Now married')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		if now_married_text is not None:
 			now_married = float((now_married_text.strip()).replace("%", ""))
 		else:
 			now_married = None		

 		separated_text = response.xpath("//*[contains(text(), 'Separated')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		if separated_text is not None:
 			separated = float((separated_text.strip()).replace("%", ""))
 		else:
 			separated = None		

 		widowed_text = response.xpath("//*[contains(text(), 'Widowed')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		if widowed_text is not None:
 			widowed = float((widowed_text.strip()).replace("%", ""))
 		else:
 			widowed = None

 		divorced_text = response.xpath("//*[contains(text(), 'Divorced')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		if divorced_text is not None:
 			divorced = float((divorced_text.strip()).replace("%", ""))
 		else:
 			divorced = None 

 		population_15_over_obj = { 'never_married': never_married, 'now_married': now_married, 'separated': separated, 'widowed': widowed, 'divorced': divorced}
        
 		#print population_15_over_obj
 		return population_15_over_obj
 	#-------------------------------------
 	def build_taxes_extra_obj(self, response):
 		#Average Adjusted Gross Income (AGI) in 2012
 		zip_avg_AGI_text = response.xpath("//*[contains(text(), 'Average Adjusted Gross Income (AGI) in 2012')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[1]/td[2]/text()").extract_first() 
 		if zip_avg_AGI_text is not None:
 			zip_avg_AGI = int(zip_avg_AGI_text[1:].strip().replace(",",""))
 		else:
 			zip_avg_AGI = None
 		state_avg_AGI_text = response.xpath("//*[contains(text(), 'Average Adjusted Gross Income (AGI) in 2012')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[2]/td[2]/text()").extract_first()
 		if state_avg_AGI_text is not None:
 			state_avg_AGI = int(state_avg_AGI_text[1:].strip().replace(",",""))
 		else:
 			state_avg_AGI = None

 		#Salary/wage reported on 79.5% of returns
 		zip_salary_wage_text = response.xpath("//*[contains(text(), 'Salary/wage')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[1]/td[2]/text()").extract_first()
 		if zip_salary_wage_text is not None:
 			zip_salary_wage = int(zip_salary_wage_text[1:].strip().replace(",",""))
 		else:
 			zip_salary_wage = None
 		state_salary_wage_text = response.xpath("//*[contains(text(), 'Salary/wage')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[2]/td[2]/text()").extract_first()
 		if state_salary_wage_text is not None: 
 			state_salary_wage = int(state_salary_wage_text[1:].strip().replace(",",""))
 		else:
 			state_salary_wage = None

 		#Taxable interest for individuals reported on 36.5% of returns
 		zip_taxable_interest_text = response.xpath("//*[contains(text(), 'Taxable interest for individuals')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[1]/td[2]/text()").extract_first()
 		if zip_taxable_interest_text is not None:
 			zip_taxable_interest = int(zip_taxable_interest_text[1:].strip().replace(",",""))
 		else:
 			zip_taxable_interest = None

 		state_taxable_interest_text = response.xpath("//*[contains(text(), 'Taxable interest for individuals')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[2]/td[2]/text()").extract_first()
 		if state_taxable_interest_text is not None: 
 			state_taxable_interest = int(state_taxable_interest_text[1:].strip().replace(",",""))
 		else:
 			state_taxable_interest = None

 		#Ordinary dividends reported on 20.8% of returns
 		zip_ordinary_dividends_text = response.xpath("//*[contains(text(), 'Ordinary dividends')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[1]/td[2]/text()").extract_first()
 		if zip_ordinary_dividends_text is not None:
 			zip_ordinary_dividends = int(zip_ordinary_dividends_text[1:].strip().replace(",",""))
 		else:
 			zip_ordinary_dividends = None

 		state_ordinary_dividends_text = response.xpath("//*[contains(text(), 'Ordinary dividends')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[2]/td[2]/text()").extract_first()
 		if state_ordinary_dividends_text is not None:
 			state_ordinary_dividends = int(state_ordinary_dividends_text[1:].strip().replace(",",""))
 		else:
 			state_ordinary_dividends = None

 		#Net capital gain/loss in AGI reported on 18.7% of returns
 		zip_net_capital_gain_loss_text = response.xpath("//*[contains(text(), 'Net capital gain/loss in AGI')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[1]/td[2]/text()").extract_first()
 		if zip_net_capital_gain_loss_text is not None:
 			zip_net_capital_gain_loss = int(zip_net_capital_gain_loss_text[2:].strip().replace(",",""))
 			zip_net_capital_ind = zip_net_capital_gain_loss_text[:zip_net_capital_gain_loss_text.find("$")].strip()
 		else:
 			zip_net_capital_gain_loss = None
 			zip_net_capital_ind = None

 		state_net_capital_gain_loss_text = response.xpath("//*[contains(text(), 'Net capital gain/loss in AGI')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[2]/td[2]/text()").extract_first()
 		if state_net_capital_gain_loss_text is not None: 
 			state_net_capital_gain_loss = int(state_net_capital_gain_loss_text[2:].strip().replace(",",""))
 			state_net_capital_ind = state_net_capital_gain_loss_text[:state_net_capital_gain_loss_text.find("$")].strip()
 		else:
 			state_net_capital_gain_loss = None
 			state_net_capital_ind = None

 		#Profit/loss from business reported on 22.2% of returns
 		zip_profit_loss_text = response.xpath("//*[contains(text(), 'Profit/loss from business')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[1]/td[2]/text()").extract_first()
 		if zip_profit_loss_text is not None: 
 			zip_profit_loss = int(zip_profit_loss_text[2:].strip().replace(",",""))
 			zip_profit_loss_ind = zip_profit_loss_text[:zip_profit_loss_text.find("$")].strip()
 		else:
 			zip_profit_loss = None
 			zip_profit_loss_ind = None

 		state_profit_loss_text = response.xpath("//*[contains(text(), 'Profit/loss from business')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[2]/td[2]/text()").extract_first()
 		if state_profit_loss_text is not None: 
 			state_profit_loss = int(state_profit_loss_text[2:].strip().replace(",",""))
 			state_profit_loss_ind = state_profit_loss_text[:state_profit_loss_text.find("$")].strip()
 		else:
 			state_profit_loss = None
 			state_profit_loss_ind = None

 		#Taxable individual retirement arrangement distribution reported on 11.7% of returns
 		zip_retirement_arrangement_text = response.xpath("//*[contains(text(), 'Taxable individual retirement arrangement distribution')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[1]/td[2]/text()").extract_first() 
 		if zip_retirement_arrangement_text is not None:
 			zip_retirement_arrangement = int(zip_retirement_arrangement_text[1:].strip().replace(",",""))
 		else:
 		 	zip_retirement_arrangement = None	

 		state_retirement_arrangement_text = response.xpath("//*[contains(text(), 'Taxable individual retirement arrangement distribution')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[2]/td[2]/text()").extract_first()
 		if state_retirement_arrangement_text is not None: 
 			state_retirement_arrangement = int(state_retirement_arrangement_text[1:].strip().replace(",",""))
 		else:
 		 	state_retirement_arrangement = None	

 		#Total itemized deductions 23% of AGI, reported on 50.3% of returns
 		zip_itemized_deductions_text = response.xpath("//*[contains(text(), 'Total itemized deductions')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[1]/td[2]/text()").extract_first() 
 		if zip_itemized_deductions_text is not None:
 			zip_itemized_deductions = int(zip_itemized_deductions_text[1:].strip().replace(",",""))
 		else:
 			zip_itemized_deductions = None 		

 		state_itemized_deductions_text = response.xpath("//*[contains(text(), 'Total itemized deductions')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[2]/td[2]/text()").extract_first()
 		if state_itemized_deductions_text is not None: 
 			state_itemized_deductions = int(state_itemized_deductions_text[1:].strip().replace(",",""))
 		else:
 			state_itemized_deductions = None

 		#Charity contributions reported on 40.9% of returns	
 		zip_charity_contributions_text = response.xpath("//*[contains(text(), 'Charity contributions')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[1]/td[2]/text()").extract_first()
 		if zip_charity_contributions_text is not None: 
 			zip_charity_contributions = int(zip_charity_contributions_text[1:].strip().replace(",",""))
 		else:
 			zip_charity_contributions = None

 		state_charity_contributions_text = response.xpath("//*[contains(text(), 'Charity contributions')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[2]/td[2]/text()").extract_first()
 		if state_charity_contributions_text is not None: 
 			state_charity_contributions = int(state_charity_contributions_text[1:].strip().replace(",",""))
 		else:
 			state_charity_contributions = None

		#Taxes paid reported on 50.0% of returns
 		zip_taxes_paid_text = response.xpath("//*[contains(text(), 'Taxes paid')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[1]/td[2]/text()").extract_first()
 		if zip_taxes_paid_text is not None: 
 			zip_taxes_paid = int(zip_taxes_paid_text[1:].strip().replace(",",""))
 		else:
 			zip_taxes_paid = None

 		state_taxes_paid_text = response.xpath("//*[contains(text(), 'Taxes paid')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[2]/td[2]/text()").extract_first()
 		if state_taxes_paid_text is not None:
 			state_taxes_paid = int(state_taxes_paid_text[1:].strip().replace(",",""))
 		else:
 			state_taxes_paid = None

 		#Earned income credit reported on 13.2% of returns
 		zip_earned_income_text = response.xpath("//*[contains(text(), 'Earned income credit')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[1]/td[2]/text()").extract_first() 
 		if zip_earned_income_text is not None:
 			zip_earned_income = int(zip_earned_income_text[1:].strip().replace(",",""))
 		else:
 			zip_earned_income = None 		

 		state_earned_income_text = response.xpath("//*[contains(text(), 'Earned income credit')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[2]/td[2]/text()").extract_first()
 		if state_earned_income_text is not None: 
 			state_earned_income = int(state_earned_income_text[1:].strip().replace(",",""))
 		else:
 			state_earned_income = None


 		taxes_extra_obj = {'avg_AGI': {'zip': zip_avg_AGI, 'state': state_avg_AGI },
 					 'salary_wage': {'zip': zip_salary_wage, 'state': state_salary_wage },
 					 'taxable_interest': {'zip': zip_taxable_interest, 'state': state_taxable_interest},
 					 'ordinary_dividends': {'zip': zip_ordinary_dividends, 'state': state_ordinary_dividends },
 					 'net_capital_gain_loss': {'zip': zip_net_capital_gain_loss, 'state': state_net_capital_gain_loss, 'indicator': [zip_net_capital_ind, state_net_capital_ind]},
 					 'profit_loss_business': {'zip': zip_profit_loss, 'state': state_profit_loss, 'indicator': [zip_profit_loss_ind, state_profit_loss_ind]},
 					 'retirement_arrangement': {'zip': zip_retirement_arrangement, 'state': state_retirement_arrangement}, 
 					 'itemized_deductions': {'zip': zip_itemized_deductions, 'state': state_itemized_deductions}, 
 					 'charity_contributions': {'zip': zip_charity_contributions, 'state': state_charity_contributions},
 					 'taxes_paid': {'zip': zip_taxes_paid, 'state': state_taxes_paid}, 
 					 'earned_income': {'zip': zip_earned_income, 'state': state_earned_income}
 					}
 					  
 		#print taxes_extra_obj
 		return taxes_extra_obj

 	#-------------------------------------
 	def build_travel_time_obj(self, response):
 		travel_time_list = response.xpath("//*[contains(text(), 'Travel time to work (commute) in zip code')]/../following-sibling::li[not(contains(@class,'text-center'))]/text()").extract()
 		#travel_time_list = response.css("ul.list-group:nth-child(159) li.list-group-item::text").extract()
 		travel_num_text = response.xpath("//*[contains(text(), 'Travel time to work (commute) in zip code')]/../following-sibling::li[not(contains(@class,'text-center'))]/span/text()").extract()
 		#travel_num_text = response.css("ul.list-group:nth-child(159) li.list-group-item span::text").extract() 
 		#--------Travel time to work (commute) in zip--------
 		travel_time_dict = {}
 		for indx, travel_time in enumerate(travel_time_list):
 			travel_num = travel_num_text[indx]
 			travel_num_value = int(travel_num.strip().replace(",", ""))
 		
 			travel_time_key = travel_time.lower().strip().replace(" ","_")
 			travel_time_dict[travel_time_key] = travel_num_value
		  
 		#print travel_time_dict
 		return travel_time_dict
 	#-------------------------------------
 	def build_heating_fuel_obj(self, response):
 		heating_types_list = response.xpath("//*[contains(text(), 'Most commonly used house heating fuel')]/../following-sibling::ul[contains(@class,'list-group')]/li[contains(@class,'list-group-item')]/b/text()").extract()
 		#heating_types_list = response.css("div.col-md-6:nth-child(185) > div:nth-child(1) > ul:nth-child(2) li.list-group-item b::text").extract()
 		heating_per_text = response.xpath("//*[contains(text(), 'Most commonly used house heating fuel')]/../following-sibling::ul[contains(@class,'list-group')]/li[contains(@class,'list-group-item')]/span/text()").extract()
 		#heating_per_text = response.css("div.col-md-6:nth-child(185) > div:nth-child(1) > ul:nth-child(2) li.list-group-item span::text").extract() 
 		#--------Most commonly used house heating fuel--------
 		heating_types_dict = {}
 		for indx, heating_type in enumerate(heating_types_list):
 			heating_per = heating_per_text[indx]
 			heating_per_value = float(heating_per.strip().replace("%",""))
 		
 			heating_type_key = heating_type.lower().strip().replace(" ","_").replace(",","")
 			heating_types_dict[heating_type_key] = heating_per_value
		  
 		#print heating_types_dict
 		return heating_types_dict
 	#-------------------------------------
 	def build_school_enrollment_obj(self, response):
 		#Private vs. public school enrollment
 		elementary_middle_school_text = response.xpath("//*[contains(text(), 'Students in private schools in grades 1 to 8 (elementary and middle school)')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		if elementary_middle_school_text is not None:
 			elementary_middle_school = int(elementary_middle_school_text.strip())
 		else:
 			elementary_middle_school = None

 		zip_elementary_middle_school_text = response.xpath("//*[contains(text(), 'Students in private schools in grades 1 to 8 (elementary and middle school)')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[1]/td[2]/text()").extract_first() 
 		zip_elementary_middle_school = float(zip_elementary_middle_school_text.strip().replace("%",""))
 		state_elementary_middle_school_text = response.xpath("//*[contains(text(), 'Students in private schools in grades 1 to 8 (elementary and middle school)')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[2]/td[2]/text()").extract_first() 
 		state_elementary_middle_school = float(state_elementary_middle_school_text.strip().replace("%","")) 		

 		high_school_text = response.xpath("//*[contains(text(), 'Students in private schools in grades 9 to 12 (high school)')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		if high_school_text is not None:
 			high_school = int(high_school_text.strip())
 		else:
 			high_school = None

 		zip_high_school_text = response.xpath("//*[contains(text(), 'Students in private schools in grades 9 to 12 (high school)')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[1]/td[2]/text()").extract_first() 
 		zip_high_school = float(zip_high_school_text.strip().replace("%",""))
 		state_high_school_text = response.xpath("//*[contains(text(), 'Students in private schools in grades 9 to 12 (high school)')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[2]/td[2]/text()").extract_first() 
 		state_high_school = float(state_high_school_text.strip().replace("%","")) 		


 		colleges_text = response.xpath("//*[contains(text(), 'Students in private undergraduate colleges')]/following-sibling::node()[1]/self::text()[normalize-space()]").extract_first()
 		if colleges_text is not None:
 			colleges = int(colleges_text.strip())
 		else:
 			colleges = None

 		zip_colleges_text = response.xpath("//*[contains(text(), 'Students in private undergraduate colleges')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[1]/td[2]/text()").extract_first() 
 		zip_colleges = float(zip_colleges_text.strip().replace("%",""))
 		state_colleges_text = response.xpath("//*[contains(text(), 'Students in private undergraduate colleges')]/following-sibling::div[contains(@class,'hgraph')]/table/tr[2]/td[2]/text()").extract_first() 
 		state_colleges = float(state_colleges_text.strip().replace("%","")) 		

 		enrollment_obj = { 'elementary_middle_school': {'number': elementary_middle_school, 'percent': {'zip': zip_elementary_middle_school, 'state': state_elementary_middle_school } }, 'high_school': {'number': high_school, 'percent': {'zip': zip_high_school, 'state': state_high_school } }, 'colleges': {'number': colleges, 'percent': {'zip': zip_colleges, 'state': state_colleges } } }
        
 		#print enrollment_obj
 		return enrollment_obj
 	#-------------------------------
 	def build_occupation_obj(self, response):
 		occupation_types_list = response.xpath("//*[contains(text(), 'Occupation by median earnings in the past 12 months')]/../following-sibling::ul[contains(@class,'list-group')]/li[contains(@class,'list-group-item')]/b/text()").extract()
 		#occupation_types_list = response.css("div.col-md-8:nth-child(208) > div:nth-child(1) > ul:nth-child(2) li.list-group-item b::text").extract()
 		occupation_num_text = response.xpath("//*[contains(text(), 'Occupation by median earnings in the past 12 months')]/../following-sibling::ul[contains(@class,'list-group')]/li[contains(@class,'list-group-item')]/span/text()").extract()
 		#occupation_num_text = response.css("div.col-md-8:nth-child(208) > div:nth-child(1) > ul:nth-child(2) li.list-group-item span::text").extract() 
 		#--------Occupation by median earnings in the past 12 months--------
 		occupation_types_dict = {}
 		for indx, occupation_type in enumerate(occupation_types_list):
 			occupation_num = occupation_num_text[indx]
 			occupation_num_value = int(occupation_num.strip().replace(",",""))
 		
 			occupation_type_key = occupation_type.lower().strip().replace(" ","_").replace(",","")
 			occupation_types_dict[occupation_type_key] = occupation_num_value
		  
 		#print occupation_types_dict
 		return occupation_types_dict
 	#-------------------------------------
 	def build_accident_obj(self, response):
 		accident_types_list = response.xpath("//*[contains(text(), 'Fatal accident statistics in 2014')]/following-sibling::ul/li/b/text()").extract() 
 		
 		accident_num_text = response.xpath("//*[contains(text(), 'Fatal accident statistics in 2014')]/following-sibling::ul/li/text()").extract() 
 		#--------Fatal accident statistics in 2014--------
 		accident_types_dict = {}
 		for indx, accident_type in enumerate(accident_types_list):
 			accident_num = accident_num_text[indx]
 			accident_num_value = int(accident_num.strip().replace(",",""))
 		
 			accident_type_key = accident_type.lower().strip().replace(" ","_").replace(",","").replace(":","")
 			accident_types_dict[accident_type_key] = accident_num_value
		  
 		#print accident_types_dict
 		return accident_types_dict
 	#-------------------------------------
 	def build_transportation_obj(self, response):
 		transportation_means_list = response.xpath("//*[contains(text(), 'Means of transportation to work in zip code')]/../following-sibling::li[not(contains(@class,'text-center'))]/text()").extract()
 		#transportation_means_list = response.css("ul.list-group:nth-child(158) li.list-group-item::text").extract()
 		transportation_num_text = response.xpath("//*[contains(text(), 'Means of transportation to work in zip code')]/../following-sibling::li[not(contains(@class,'text-center'))]/span[not(contains(@class,'alert-info'))]/text()").extract()
 		#transportation_num_text = response.css("ul.list-group:nth-child(158) li.list-group-item span.badge::text").extract()
 		#print transportation_num_text
 		#transportation_num_text = transportation_num_text[1::2]
 		transportation_per_text = response.xpath("//*[contains(text(), 'Means of transportation to work in zip code')]/../following-sibling::li[not(contains(@class,'text-center'))]/span[contains(@class,'alert-info')]/text()").extract()
 		#transportation_per_text = response.css("ul.list-group:nth-child(158) li.list-group-item span.badge.alert-info::text").extract() 
 		
 		#--------Means of transportation to work in zip--------
 		transportation_means_dict = {}
 		for indx, transportation_mean in enumerate(transportation_means_list):
 			transportation_num = transportation_num_text[indx]
 			transportation_num_value = int(transportation_num.strip().replace(",", ""))
 			transportation_per = transportation_per_text[indx]
 			transportation_per_value = float(transportation_per.strip().replace("%", ""))
 		
 			transportation_means_key = transportation_mean.lower().strip().replace(" ","_")
 			transportation_means_dict[transportation_means_key] = {'number': transportation_num_value, 'percent': transportation_per_value}
		  
 		#print transportation_means_dict
 		return transportation_means_dict
 	#----------------------------------
 	def build_household_income_obj(self, response):
 		household_income_range_list = response.xpath("//*[contains(text(), 'household income distribution in 2016')]/../following-sibling::li[not(contains(@class,'text-center'))]/text()").extract()
 		#household_income_range_list = response.css("ul.list-group:nth-child(133) li.list-group-item::text").extract()
 		household_income_range_num = response.xpath("//*[contains(text(), 'household income distribution in 2016')]/../following-sibling::li[not(contains(@class,'text-center'))]/span[contains(@class,'badge')]/text()").extract()
 		#household_income_range_num = response.css("ul.list-group:nth-child(133) li.list-group-item span.badge::text").extract() 
 		
 		#--------Zip code household income distribution in 2016--------
 		household_income_range_dict = {}
 		for indx, household_income_range in enumerate(household_income_range_list):
 			household_income_range_text = household_income_range_num[indx]
 			household_income_range_num_value = int(household_income_range_text.strip().replace(",", ""))
 			
 			household_income_range_key = household_income_range.lower().strip().replace(" ","_")
 			household_income_range_dict[household_income_range_key] = household_income_range_num_value
		  
 		#print household_income_range_dict
 		return household_income_range_dict

 	#------------------------------------
 	def build_notable_locations_obj(self, response):
 		notable_locations_dict = {}
 		notable_locations_raw = response.xpath("//*[contains(text(), 'Notable locations in zip code')]/../../text()").extract()
 		#notable_locations = response.css("ul.row > li:nth-child(3)").xpath(".//p[1]/text()").extract()
 		notable_locations = []
 		for loc_indx in range(len(notable_locations_raw)-2):
 			notable_locations.append(notable_locations_raw[loc_indx].replace(",","").strip())
 		#print notable_locations
 		notable_locations_dict['locations'] = notable_locations 		

 		location_key_elements = response.xpath("//*[contains(text(), 'Notable locations in zip code')]/../../following-sibling::p")
 		for indx, element in enumerate(location_key_elements):
 			loc_key = element.xpath(".//b/font/text()").extract_first()
 			loc_key = loc_key.replace(":","").strip().lower()
 			if "(" in loc_key:
 				loc_key = loc_key[:loc_key.rfind("(")-1]
 			if "in zip code" in loc_key:	 
 				loc_key = loc_key[:loc_key.rfind("in zip code")-1]
 			loc_key = loc_key.replace(" ", "_") 
 			places = element.xpath(".//text()").extract()
 			key_locations = []
 			for loc_indx in range(len(places)-3):
 				if loc_indx % 2 != 0:
 					key_locations.append(places[loc_indx].replace(",","").strip())
 			notable_locations_dict[loc_key] = key_locations
 			
 		#print notable_locations_dict
 		return notable_locations_dict

#-----------END of CLASS----------------

