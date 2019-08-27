import scrapy
import re

file_path = "/home/tayeen/Yelp/city_data/"


class CountySpider(scrapy.Spider):
 	name = "countyinfo"

 	with open(file_path+"AZ_counties.txt", "rt") as f:
 		start_urls = ["http://www.city-data.com/county/"+county.strip()+"_County-AZ.html" for county in f.readlines()]
 	#start_urls = ['http://www.city-data.com/county/Pinal_County-AZ.html']
 	    
 	def start_requests(self):
 		for indx, url in enumerate(self.start_urls):
 			yield scrapy.Request(url=url, callback=self.parse, meta={'input_url': url, 'url_index': indx})
 

 	def parse(self, response):
 		in_url = response.meta['input_url']
 		url_indx = response.meta['url_index'] 

 		county_id = in_url[in_url.rfind("/")+1: len(in_url)-5]
 		print "=================================="
 		print county_id
 		print "==================================="
 		population_info = self.build_population_obj(response) #county_population
 		housing_info = self.build_housing_obj(response) #county_housing
 		density_info = self.build_density_obj(response) #county_density

 		#-------------Mar. 2016 cost of living index----------------- 
 		cost_of_living = float((response.xpath("//section[@id='cost-of-living']/text()").extract_first()).strip())  
 		#print "Cost: %f"%cost_of_living
 		#--------------------------------------------------------------

 		employment_info = self.build_employment_structure(response) #county_employment-structure
 		races_info = self.build_races_obj(response) #county_races

 		#-------------median resident age-----------
 		county_median_age_text = response.xpath("//section[@id='median-age']/div/table/tr[1]/td[2]/text()").extract_first()
 		county_median_age = float(county_median_age_text[0:county_median_age_text.rfind("years")-1].strip())
 		#print county_median_age
 		state_median_age_text = response.xpath("//section[@id='median-age']/div/table/tr[2]/td[2]/text()").extract_first()
 		state_median_age = float(state_median_age_text[0:state_median_age_text.rfind("years")-1].strip())
 		#print state_median_age
 		#-------------------------------------------
 
 		#--------------population by sex-------------
 		males_text = response.xpath("//section[@id='population-by-sex']/div/table/tr[1]/td[1]/text()").extract_first()
 		males_per_text = response.xpath("//section[@id='population-by-sex']/div/table/tr[1]/td[2]/text()").extract_first()		
 		males_num = int((males_text.strip()).replace(",", ""))
 		males_per = float(males_per_text[males_per_text.find("(")+1:males_per_text.find("%")].strip())
 		#print ("%d, %f")%(males_num, males_per)
 		females_text = response.xpath("//section[@id='population-by-sex']/div/table/tr[2]/td[1]/text()").extract_first()
 		females_per_text = response.xpath("//section[@id='population-by-sex']/div/table/tr[2]/td[2]/text()").extract_first()	
 		females_num = int((females_text.strip()).replace(",", ""))
 		females_per = float(females_per_text[females_per_text.find("(")+1:females_per_text.find("%")].strip())		
 		#print ("%d, %f")%(females_num, females_per)
 		#---------------------------------------------
 		household_info = self.build_household_obj(response)        

 		yield {'county_name': county_id, 'population': population_info, 'housing': housing_info, 'density': density_info, 'cost_of_living': cost_of_living, 'employment': employment_info, 'races': races_info, 'county_median_age': county_median_age, 'state_median_age': state_median_age, 'males_num': males_num, 'males_per': males_per, 'females_num':females_num, 'females_per': females_per, 'household': household_info, } 
 		
    
 	#------------------------------------
 	def build_population_obj(self, response):
 		attribute = response.xpath("//section[@id='population']/b/text()").extract_first()
 		value = response.xpath("//section[@id='population']/text()").extract_first() 
 		#print population_attr
 		#print population_val
 		#total_population_text = total_population_text.replace(" ", "_")		#re.sub("\s+", " ", total_population_key).strip() 
 		total_2016_val = int((value[0:value.find("(")].strip()).replace(",", ""))
 		total_2016_urban = int(value[value.find("(")+1:value.find("%")].strip())
 		total_2016_rural = int(value[value.find("urban,")+7:value.find("rural)")-2].strip())
 		total_2000_val = int((value[value.find("was")+4:value.find("in")].strip()).replace(",", ""))
 		
 		population_obj = {'2016': {'total': total_2016_val, 'urban_percent': total_2016_urban, \
                                  'rural_percent': total_2016_rural }, \
 						  '2000': {'total': total_2000_val}
                         }

 		#print population_obj
 		return population_obj
 	#-------------------------------------
 	def build_housing_obj(self, response):
 		attributes = response.xpath("//section[@id='housing']/b/text()").extract()
 		values = response.xpath("//section[@id='housing']/text()").extract()     		
 		#print attributes
 		#print values
 		mortgage_loan = int((values[0].strip()).replace(",", "")) #County owner-occupied with a mortgage or a loan houses and condos in 2010 
 		free_clear = int((values[2].strip()).replace(",", "")) #County owner-occupied free and clear houses and condos in 2010 
 		renter_apt_2010 = int((values[6][0:values[6].find("(")].strip()).replace(",", "")) #Renter-occupied apartments in 2010
 		renter_apt_2000 = int((values[6][values[6].find("was")+4:values[6].find("in")].strip()).replace(",", "")) #Renter-occupied apartments in 2000
 		houses_condos = int((values[4].strip()).replace(",", "")) #County owner-occupied houses and condos in 2000
 		
 		# % of renters in county
 		#county_renters = response.xpath("//section[@id='housing']/div[contains(@class,'hgraph')]/p[@class='h']/text()").extract_first()
 		# % of renters in state
 		#state_renters = response.xpath("//section[@id='housing']/div[contains(@class,'hgraph')]/p[@class='a']/text()").extract_first()  

 		housing_obj = {'2010': {'mortgage_loan': mortgage_loan, 'free_clear': free_clear, \
                                  'renter_apt': renter_apt_2010 }, \
 					   '2000': {'houses_condos': houses_condos, 'renter_apt': renter_apt_2000}
 					   #'county_renter_percent': county_renters, 'state_renter_percent': state_renters  
                      }

 		#print housing_obj
 		return housing_obj
 	#-------------------------------------
 	def build_density_obj(self, response):
 		attributes = response.xpath("//section[@id='population-density']/p/b/text()").extract()
 		values = response.xpath("//section[@id='population-density']/p/text()").extract()     		
 		#print attributes
 		#print values
 		land_area = int((values[0][0:values[0].find("sq")].strip()).replace(",", "")) #Land area in square mile
 		water_area = float((values[1][0:values[1].find("sq")].strip()).replace(",", "")) 	#Water area in square mile
 		population_density = int((values[2][0:values[2].find("people")].strip()).replace(",", "")) #people per square mile in average
 		
 		density_obj = {'land_area_sq_mi': land_area, 'water_area_sq_mi': water_area, 'population_density': population_density }
 					  
 		#print density_obj
 		return density_obj
 	#-------------------------------------
 	def build_employment_structure(self, response):
 		attributes = response.xpath("//section[@id='employment-structure']/p/b/text()").extract() 
 		values = response.xpath("//section[@id='employment-structure']/p/text()").extract()     		
 		#print attributes
 		#print values
 		#--------Industries providing employment-------- 
 		scientific_management_admin = float((values[0][values[0].find("(")+1:values[0].find(")")-1].strip()).replace(",", "")) #Professional, scientific, management, administrative, and waste management services

 		educational_health_social = float((values[1][values[1].find("(")+1:values[1].find(")")-1].strip()).replace(",", "")) #Educational, health and social services

 		finance_insurance_rental = float((values[2][values[2].find("(")+1:values[2].find(")")-1].strip()).replace(",", "")) #Finance, insurance, real estate, and rental and leasing

 		#--------Type of workers--------
 		attributes = response.xpath("//section[@id='employment-structure']/div/ul/li/b/text()").extract() 
 		values = response.xpath("//section[@id='employment-structure']/div/ul/li/text()").extract()     		
 		#print attributes
 		#print values
 		private_wage_salary = float((values[0][0:values[0].find("%")].strip()).replace(",", "")) #Private wage or salary
 		government = float((values[1][0:values[1].find("%")].strip()).replace(",", "")) #Government
 		self_employed = float((values[2][0:values[2].find("%")].strip()).replace(",", "")) #Self-employed, not incorporated
 		unpaid_family_work = float((values[3][0:values[3].find("%")].strip()).replace(",", "")) #Unpaid family work

 		employment_obj = { 'industries': {'sci_man_admin_percent': scientific_management_admin, 'edu_health_social_percent': educational_health_social, 'fin_insur_rental': finance_insurance_rental }, \
 				'worker_types': { 'priv_wage_percent': private_wage_salary, 'gov': government, 'self': self_employed, 'unpaid': unpaid_family_work }
 						}
 					  
 		#print employment_obj
 		return employment_obj
 	#-------------------------------------
 	def build_races_obj(self, response):
 		attributes = response.xpath("//section[@id='races']/div/ul/li/b/text()").extract() 
 		values = response.xpath("//section[@id='races']/div/ul/li/text()").extract()     		
 		#print attributes
 		#print values
 		#--------races--------
 		races_per = []
 		for indx, val in enumerate(values):
 			 races_per.append(float((val[val.find("(")+1:val.find(")")-1].strip()).replace(",", "")))

 		#White Non-Hispanic Alone, Hispanic or Latino, American Indian and Alaska Native alone, Black Non-Hispanic Alone, Asian alone, 
 		#Two or more races, Native Hawaiian and Other Pacific Islander alone, Some other race alone
 		races_obj = {'white_non_hispanic': races_per[0], 'hispanic_latino': races_per[1], 'american_indian_alaska': races_per[2], 'black_non_hispanic': races_per[3], 'asian_alone': races_per[4], 'two_or_more_races': races_per[5], 'native_hawaiian_pacific_islander': races_per[6], 'other_race': races_per[7]}
				  
 		#print races_obj
 		return races_obj
 	#-------------------------------------
 	def build_household_obj(self, response):
 		attributes = response.xpath("//section[@id='household-prices']/b/text()").extract() 
 		values = response.xpath("//section[@id='household-prices']/text()").extract()     		 		
 		print attributes		
 		print values

 		#Average household size
 		county_avg_house_size_text = response.xpath("//section[@id='household-prices']/div[1]/table/tr[1]/td[2]/text()").extract_first() 
 		county_avg_household_size = float(county_avg_house_size_text[0:county_avg_house_size_text.find("people")].strip())
 		state_avg_house_size_text = response.xpath("//section[@id='household-prices']/div[1]/table/tr[2]/td[2]/text()").extract_first() 
 		state_avg_household_size = float(state_avg_house_size_text[0:state_avg_house_size_text.find("people")].strip())
 		
 		#Estimated median household income in 2016, 1999
 		county_median_house_income_1999 = int((values[2][values[2].rfind("$")+1:values[2].find("in")-1].strip()).replace(",", "")) 

 		county_median_house_income_text = response.xpath("//section[@id='household-prices']/div[2]/table/tr[1]/td[2]/text()").extract_first() 
 		county_median_house_income = int((county_median_house_income_text[1:len(county_median_house_income_text)].strip()).replace(",", ""))
 		
 		state_median_house_income_text = response.xpath("//section[@id='household-prices']/div[2]/table/tr[2]/td[2]/text()").extract_first() 
 		state_median_house_income = int((state_median_house_income_text[1:len(state_median_house_income_text)].strip()).replace(",", ""))
 		
 		#Median contract rent in 2016 for apartments
 		county_median_contract_rent_text = response.xpath("//section[@id='household-prices']/div[3]/table/tr[1]/td[2]/text()").extract_first() 
 		county_median_contract_rent = int((county_median_contract_rent_text[1:len(county_median_contract_rent_text)].strip()).replace(",", ""))
 		state_median_contract_rent_text = response.xpath("//section[@id='household-prices']/div[3]/table/tr[2]/td[2]/text()").extract_first() 
 		state_median_contract_rent = int((state_median_contract_rent_text[1:len(state_median_contract_rent_text)].strip()).replace(",", ""))
 
 		#Estimated median house or condo value in 2016
 		county_median_house_value_text = response.xpath("//section[@id='household-prices']/div[4]/table/tr[1]/td[2]/text()").extract_first() 
 		county_median_house_value = int((county_median_house_value_text[1:len(county_median_house_value_text)].strip()).replace(",", ""))
 		state_median_house_value_text = response.xpath("//section[@id='household-prices']/div[4]/table/tr[2]/td[2]/text()").extract_first() 
 		state_median_house_value = int((state_median_house_value_text[1:len(state_median_house_value_text)].strip()).replace(",", ""))

 		#--------Mean price in 2016--------
 		div_selectors = response.xpath("//section[@id='household-prices']/blockquote/div")
 		#Detached houses, Townhouses or other attached units, In 2-unit structures, In 3-to-4-unit structures, In 5-or-more-unit structures, 
 		#Mobile homes, Occupied boats, RVs, vans, etc
 		mean_prices = []
 		for indx, selector in enumerate(div_selectors):
 			county_mean_text = div_selectors[indx].xpath(".//table/tr[1]/td[2]/text()").extract_first()
 			state_mean_text =  div_selectors[indx].xpath(".//table/tr[2]/td[2]/text()").extract_first()
			#print ("%s, %s")%(county, state)
 			county_mean = int((county_mean_text[1:len(county_mean_text)].strip()).replace(",", ""))
 			state_mean = int((state_mean_text[1:len(state_mean_text)].strip()).replace(",", ""))
 			mean_prices.append((county_mean, state_mean))

 		#Median monthly housing costs for homes and condos with a mortgage
 		median_monthly_costs_w_mortgage_text = values[len(values)-5]
 		#print median_monthly_costs_w_mortgage_text
 		median_monthly_costs_w_mortgage = int((median_monthly_costs_w_mortgage_text[1:len(median_monthly_costs_w_mortgage_text)].strip()).replace(",", ""))
 		median_monthly_costs_mortgage_text = values[len(values)-3]
 		#print median_monthly_costs_mortgage_text	
 		median_monthly_costs_mortgage = int((median_monthly_costs_mortgage_text[1:len(median_monthly_costs_mortgage_text)].strip()).replace(",", ""))

 		household_obj = {'avg_household_size': {'county': county_avg_household_size, 'state': state_avg_household_size}, \
 						 'median_household_income': {'county': county_median_house_income, 'state': state_median_house_income}, \
 						 'median_apt_contract_rent': {'county': county_median_contract_rent, 'state': state_median_contract_rent}, \
 						 'median_house_condo_value': {'county': county_median_house_value, 'state': state_median_house_value}, \
 						 'mean_price': {'detached_houses': {'county': mean_prices[0][0], 'state': mean_prices[0][1]}, 
 										'townhouses': {'county': mean_prices[1][0], 'state': mean_prices[1][1]}, 
  										'2-unit_structures': {'county': mean_prices[2][0], 'state': mean_prices[2][1]},
 										'3-to-4-unit_structures': {'county': mean_prices[3][0], 'state': mean_prices[3][1]},
 										'5-or-more-unit_structures': {'county': mean_prices[4][0], 'state': mean_prices[4][1]},
 										'mobilehomes_boats': {'county': mean_prices[5][0], 'state': mean_prices[5][1]}
 									   },
 						 'median_mon_cost_w_mortgage': median_monthly_costs_w_mortgage,
 						 'median_mon_cost_mortgage': median_monthly_costs_mortgage	
 						}
				  
 		#print household_obj
 		return household_obj
