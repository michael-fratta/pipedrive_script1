# python scheduler
import schedule
import time

def job(): # define the whole script as a function

    from dotenv import load_dotenv
    import os
    load_dotenv()

    # importing xlsx file from sftp as dataframe with pandas

    import pandas as pd
    from datetime import datetime
    import pysftp

    today = datetime.today().strftime('%d%m%Y %H-00')

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys=None

    hostname = os.getenv('hostname')
    username = os.getenv('username')
    password = os.getenv('password')

    # initiate file_found bool
    file_found = False

    with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
        print("Ignore the above warning!\n")
        print("Connection succesfully established ... ")
        try:
            with sftp.open(f'Hourly Report ({today}).xlsx') as f:
                df = pd.read_excel(f)
                print(f"Hourly Report ({today}).xlsx!\n")
                file_found = True
        except: 
            print("No file was found!")

    if file_found:

        print("Cycling through all Deals in file... This may take a while!\n")

        # MAP keys

        stage_id = "pipedrive_field_key"
        est_del_date = "pipedrive_field_key"
        driver_agree_bool = "pipedrive_field_key"
        driver_agree_date = "pipedrive_field_key"
        vehicle_conf_sent = "pipedrive_field_key"
        vehicle_conf_signed = "pipedrive_field_key"
        order_conf_date = "pipedrive_field_key"
        #insurance_received = "pipedrive_field_key" # not used currently
        licence_check = "pipedrive_field_key"
        #initial_payment = "pipedrive_field_key" # not used currently
        #term_months = "pipedrive_field_key" # not used currently
        contract_end = "pipedrive_field_key"

        ## New keys following AF request as of 15/2/22 ##
        registration = "pipedrive_field_key"
        make = "pipedrive_field_key"
        model = "pipedrive_field_key"
        variant = "pipedrive_field_key"
        mileage = "pipedrive_field_key"

        # MAP pipeline IDs

        ed_pipe = "pipedrive_pipeline_id"
        fleet_pipe = "pipedrive_pipeline_id"
        bch_pipe = "pipedrive_pipeline_id"

        # turning df into list for comparison by ID

        deal_list = []

        for i in df["Pipedrive Deal ID"]:
            if i > 0: # IOW if it has a Deal ID
                deal_list.append(int(i))

        # UPDATE RELEVANT FIELD
        import requests
        from datetime import datetime
        from dateutil.relativedelta import relativedelta # to add term months

        token = {'api_token': os.getenv('token')}

        fails = []
        deal_col = "Pipedrive Deal ID"
        pipeline = 0
        dupes = set()

        # initiate list to hold updated deals
        updated_deals = []

        # initiate list to hold not found deals
        unfound_deals = []
            
        for i in deal_list:
            
            if len(df.loc[df[deal_col] == i,"Status (ID)"].notnull()) > 1:
                dupes.add(i)
            else: # if not a dupe
                # get deal data
                getting_response = requests.get(f'https://your-domain.pipedrive.com/api/v1/deals/{i}', params=token)
                if getting_response.ok:
                    getting_data = getting_response.json()['data'] # convert to json
                    pipeline = getting_data['pipeline_id'] # get pipeline id
                    current_stage = getting_data['stage_id'] # get current stage
                    #term_string = getting_data[term_months] # get term_month (not used currently)

                    # # MAP term_string to relevant term_int (not used currently)
                    # if term_string == "pipedrive_field_value":
                    #     term_int = 12
                    # elif term_string == "pipedrive_field_value":
                    #     term_int = 24
                    # elif term_string == "pipedrive_field_value":
                    #     term_int = 36
                    # elif term_string == "pipedrive_field_value":
                    #     term_int = 48
                    # elif term_string == "pipedrive_field_value":
                    #     term_int = 60
                    # elif term_string == "pipedrive_field_value":
                    #     term_int = None


                    # do the following if status ID in file is not blank
                    if int(df.loc[df[deal_col] == i,"Status (ID)"].notnull()): 

                        status_id = int(df.loc[df[deal_col] == i,"Status (ID)"]) # assign status_id

                        if pipeline == ed_pipe: # if in ed signed employees pipeline
                            if status_id == "Key2 Status ID" and current_stage != "pipedrive_field_value":
                                pd_stage = "pipedrive_field_value"
                            elif status_id in ["Key2 Status IDs"] and current_stage != "pipedrive_field_value": 
                                pd_stage = "pipedrive_field_value"
                            elif status_id == "Key2 Status ID" and current_stage != "pipedrive_field_value": 
                                pd_stage = "pipedrive_field_value"
                            elif status_id in ["Key2 Status IDs"] and current_stage != "pipedrive_field_value":
                                pd_stage = "pipedrive_field_value"
                            elif status_id in ["Key2 Status IDs"] and current_stage != "pipedrive_field_value":
                                pd_stage = "pipedrive_field_value"
                            else:
                                pd_stage = None

                        elif pipeline == fleet_pipe: # if in fleet pipeline
                            if status_id in ["Key2 Status IDs"] and current_stage != "pipedrive_field_value": 
                                pd_stage = "pipedrive_field_value"
                            elif status_id in ["Key2 Status IDs"] and current_stage != "pipedrive_field_value":
                                pd_stage = "pipedrive_field_value"
                            elif status_id in ["Key2 Status IDs"] and current_stage != "pipedrive_field_value":
                                pd_stage = "pipedrive_field_value"
                            else:
                                pd_stage = None

                        elif pipeline == bch_pipe: # if in bch pipeline
                            if status_id in ["Key2 Status IDs"] and current_stage != "pipedrive_field_value": 
                                pd_stage = "pipedrive_field_value"
                            elif status_id in ["Key2 Status IDs"] and current_stage != "pipedrive_field_value":
                                pd_stage = "pipedrive_field_value"
                            elif status_id in ["Key2 Status IDs"] and current_stage != "pipedrive_field_value":
                                pd_stage = "pipedrive_field_value"
                            else:
                                pd_stage = None

                    # else if status id blank but is in ed_pipe and has conf_signed
                    elif int(df.loc[df[deal_col] == i, 
                                        "Status (ID)"].isnull()) and pipeline == ed_pipe and str(df.loc[df[deal_col] == i,
                                        "Vehicle Return Date"].iloc[0]) != "NaT":
                        if current_stage != "pipedrive_field_value":
                            pd_stage = "pipedrive_field_value"
                        else:
                            pd_stage = None

                    # else if status id blank but is in ed_pipe and has conf_signed
                    elif int(df.loc[df[deal_col] == i, "Status (ID)"].isnull()) and pipeline == ed_pipe:
                        pd_stage = None

                    # else if status id blank but is in fleet_pipe and has conf_signed
                    elif int(df.loc[df[deal_col] == i, "Status (ID)"].isnull()) and pipeline == fleet_pipe:
                        pd_stage = None

                    # else if status id blank but is in bch_pipe and has conf_signed
                    elif int(df.loc[df[deal_col] == i, "Status (ID)"].isnull()) and pipeline == bch_pipe:
                        pd_stage = None

                    ### done with status id mappings ###

                    # MAP driver_agree_bool
                    if df.loc[df[deal_col] == i, "Driver Agreement Signed"].iloc[0] == True:
                            bool_result = "pipedrive_field_value" # if true
                    else:
                            bool_result = "pipedrive_field_value" # if false

                    # check if est_del_date field is blank in file
                    if str(df.loc[df[deal_col] == i, "Order Expected On"].iloc[0]) == "NaT":
                        del_date = None # assn null if field is blank
                    else:
                        del_date = str(df.loc[df[deal_col] == i,"Order Expected On"].iloc[0].date())
                        # if term_int != None:
                        #     datetime_str = del_date
                        #     datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d') # convert date str to date obj
                        #     end_date = datetime_obj.date() + relativedelta(months=+term_int) # increment date obj by relevant term_int
                        # else:
                        #     end_date = None

                    # check if end_date field is blank in file
                    if str(df.loc[df[deal_col] == i, "Estimated End Date"].iloc[0]) == "NaT":
                        end_date = None # assn null if field is blank
                    else:
                        end_date = str(df.loc[df[deal_col] == i, "Estimated End Date"].iloc[0].date())
                    
                    
                    # check if driver_agree_date field is blank in file
                    if str(df.loc[df[deal_col] == i, "Driver Agreement Signed Date"].iloc[0]) == "NaT":
                        agree_date = None
                    else:
                        agree_date = str(df.loc[df[deal_col] == i,"Driver Agreement Signed Date"].iloc[0].date())

                    # check if vehicle_conf_sent field is blank in file
                    if str(df.loc[df[deal_col] == i, "Vehicle Schedule Received"].iloc[0]) == "NaT":
                        conf_sent = None # assn null if field is blank
                    else:
                        conf_sent = str(df.loc[df[deal_col] == i, "Vehicle Schedule Received"].iloc[0].date())

                    # check if vehicle_conf_signed field is blank in file
                    if str(df.loc[df[deal_col] == i, "Vehicle Schedule Return Date"].iloc[0]) == "NaT":
                        conf_signed = None
                    else:
                        conf_signed = str(df.loc[df[deal_col] == i, "Vehicle Schedule Return Date"].iloc[0].date())

                    # check if order_conf_date field is blank in file
                    if str(df.loc[df[deal_col] == i, "Date Ordered"].iloc[0]) == "NaT":
                        conf_date = None
                    else:
                        conf_date = str(df.loc[df[deal_col] == i, "Date Ordered"].iloc[0].date())
                        
                    # check if licence_check field is blank in file
                    if str(df.loc[df[deal_col] == i, "Licence Last Checked"].iloc[0]) == "NaT":
                        check_date = None
                    else:
                        check_date = str(df.loc[df[deal_col] == i,"Licence Last Checked"].iloc[0].date())      

                    ## NEW additions since AF req 15/2/22 ##

                    ## MAP make ##
                    doc_make = df.loc[df[deal_col] == i, "Make"].iloc[0]

                    if doc_make == "AUDI":
                        map_make = "pipedrive_field_value"
                    
                    elif doc_make == "BMW":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "CITROEN":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "CUPRA":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "FIAT":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "FORD":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "HONDA":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "HYUNDAI":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "JAGUAR":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "KIA":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "LEVC":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "LEXUS":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "MAXUS":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "MAZDA":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "MERCEDES-BENZ":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "MG MOTOR UK":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "MINI":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "NISSAN":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "PEUGEOT":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "POLESTAR":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "PORSCHE":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "RENAULT":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "SEAT":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "SKODA":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "TESLA":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "VAUXHALL":
                        map_make = "pipedrive_field_value"

                    elif doc_make == "VOLKSWAGEN":
                        map_make = "pipedrive_field_value"
                    
                    elif doc_make == "VOLVO":
                        map_make = "pipedrive_field_value"

                    else:
                        map_make = None


                    ## MAP Model ##

                    doc_model = df.loc[df[deal_col] == i, "Model"].iloc[0]

                    if doc_model == '2 FASTBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == '500 ELECTRIC CABRIO':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == '500 ELECTRIC HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'BORN ELECTRIC HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'C40 ESTATE':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'CORSA-E ELECTRIC HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'E DELIVER 3 L1 ELECTRIC':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'E DELIVER 9 LWB ELECTRIC FWD':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'e HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'E-2008 ELECTRIC ESTATE':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'E-208 ELECTRIC HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'E-C4 ELECTRIC HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'e-EXPERT STANDARD':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'ELECTRIC HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'ELECTRIC HATCHBACK SPECIAL EDITION':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'E-NIRO ELECTRIC ESTATE':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'ENYAQ IV ESTATE':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'EQA HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'EQB ESTATE':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'EQC ESTATE':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'E-TRON ESTATE':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'E-TRON GT SALOON':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'E-TRON SPORTBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'EV6 ELECTRIC ESTATE':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'I3 HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'I4 GRAN COUPE':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'ID.3 ELECTRIC HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'ID.4 ELECTRIC ESTATE':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'ID.4 ESTATE SPECIAL EDITION':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'IONIQ 5 ELECTRIC HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'IONIQ ELECTRIC HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'I-PACE ESTATE':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'I-PACE ESTATE SPECIAL EDITIONS':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'iX ESTATE':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'iX3-E ELECTRIC ESTATE':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'KONA ELECTRIC HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'LEAF HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'MG5 ELECTRIC ESTATE':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'MODEL 3 SALOON':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'MODEL S HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'MODEL X HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'MODEL Y HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'MOKKA-E ELECTRIC HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'MUSTANG MACH-E ESTATE':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'MX-30 HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'POLESTAR 2 FASTBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'Q4 E-TRON ESTATE':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'Q4 E-TRON ESTATE SPECIAL EDITIONS':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'Q4 E-TRON SPORTBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'Q4 E-TRON SPORTBACK SPECIAL EDITIONS':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'RS E-TRON GT SALOON':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'SOUL ELECTRIC HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'TAYCAN CROSS TURISMO':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'TAYCAN SALOON':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'TAYCAN SPORT TURISMO':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'UP ELECTRIC HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'UX ELECTRIC HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'XC40 ELECTRIC ESTATE':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'ZOE HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    elif doc_model == 'ZS ELECTRIC HATCHBACK':
                        map_model = 'pipedrive_field_value'

                    else:
                        map_model = None

                    
                    ## MAP Contract Mileage ##

                    # get K2 lifetime distance from file
                    if int(df.loc[df[deal_col] == i,"Distance"].notnull()): 
                        lifetime_miles = int(df.loc[df[deal_col] == i, "Distance"].iloc[0])

                        # get K2 term months from file
                        lifetime_months = int(df.loc[df[deal_col] == i, "Contract Term"].iloc[0])

                        # calculate yearly mileage 
                        yearly_miles = (lifetime_miles/lifetime_months) * 12

                        # MAP with PD yearly mileages
                        if yearly_miles == 5000:
                            yearly_id = "pipedrive_field_value"
                        elif yearly_miles == 6000:
                            yearly_id = "pipedrive_field_value"
                        elif yearly_miles == 8000:
                            yearly_id = "pipedrive_field_value"
                        elif yearly_miles == 10000:
                            yearly_id = "pipedrive_field_value"
                        elif yearly_miles == 12000:
                            yearly_id = "pipedrive_field_value"
                        elif yearly_miles == 15000:
                            yearly_id = "pipedrive_field_value"
                        elif yearly_miles == 20000:
                            yearly_id = "pipedrive_field_value"
                        elif yearly_miles == 25000:
                            yearly_id = "pipedrive_field_value"
                        elif yearly_miles == 30000:
                            yearly_id = "pipedrive_field_value"   
                        else:
                            yearly_id = "pipedrive_field_value" #custom
                    else:
                        yearly_id = None

                    
                    ## ASSIGN derivative (variant from K2 doc)
                    if str(df.loc[df[deal_col] == i, "Derivative"].iloc[0]) == "nan":
                        derivative = None
                    else:
                        derivative = df.loc[df[deal_col] == i, "Derivative"].iloc[0]

                    ## ASSIGN registration
                    if str(df.loc[df[deal_col] == i, "Registration Number"].iloc[0]) == "nan":
                        reg_number = None
                    else:
                        reg_number = df.loc[df[deal_col] == i, "Registration Number"].iloc[0]


                    # only update if need be #

                    if pd_stage == None: # comparisons already taken care of above
                        put_pd_stage = None
                    else:
                        put_pd_stage = pd_stage

                    existing_del_date = getting_data[est_del_date]
                    if existing_del_date == del_date:
                        put_del_date = None
                    else:
                        put_del_date = del_date

                    existing_bool_result = getting_data[driver_agree_bool]
                    if existing_bool_result == bool_result:
                        put_bool_result = None
                    else:
                        put_bool_result = bool_result

                    existing_agree_date = getting_data[driver_agree_date]
                    if existing_agree_date == agree_date:
                        put_agree_date = None
                    else:
                        put_agree_date = agree_date

                    existing_conf_sent = getting_data[vehicle_conf_sent]
                    if existing_conf_sent == conf_sent:
                        put_conf_sent = None
                    else:
                        put_conf_sent = conf_sent

                    existing_conf_signed = getting_data[vehicle_conf_signed]
                    if existing_conf_signed == conf_signed:
                        put_conf_signed = None
                    else:
                        put_conf_signed = conf_signed

                    existing_conf_date = getting_data[order_conf_date]
                    if existing_conf_date == conf_date:
                        put_conf_date = None
                    else:
                        put_conf_date = conf_date

                    existing_check_date = getting_data[licence_check]
                    if existing_check_date == check_date:
                        put_check_date = None
                    else:
                        put_check_date = check_date

                    existing_end_date = getting_data[contract_end]
                    if existing_end_date == end_date:
                        put_end_date = None
                    else:
                        put_end_date = end_date

                    existing_map_make = getting_data[make]
                    if existing_map_make == map_make:
                        put_map_make = None
                    else:
                        put_map_make = map_make

                    existing_map_model = getting_data[model]
                    if existing_map_model == map_model:
                        put_map_model = None
                    else:
                        put_map_model = map_model

                    existing_yearly_id = getting_data[mileage]
                    if existing_yearly_id == yearly_id:
                        put_yearly_id = None
                    else:
                        put_yearly_id = yearly_id

                    existing_derivative = getting_data[variant]
                    if existing_derivative == derivative:
                        put_derivative = None
                    else:
                        put_derivative = derivative
                    
                    existing_reg_number = getting_data[registration]
                    if existing_reg_number == reg_number:
                        put_reg_number = None
                    else:
                        put_reg_number = reg_number

                    # only create payload if need be
                    if put_pd_stage == None and put_del_date == None and put_bool_result == None and put_agree_date == None and put_conf_sent == None and \
                        put_conf_signed == None and put_conf_date == None and put_check_date == None and put_end_date == None and put_map_make == None and \
                            put_map_model == None and put_yearly_id == None and put_derivative == None and put_reg_number == None:
                            pass
                    else:
                        # assign data to dictionary for PUT
                        csv_data = {
                            stage_id: put_pd_stage,
                            est_del_date: put_del_date,
                            driver_agree_bool: put_bool_result,
                            driver_agree_date: put_agree_date,
                            vehicle_conf_sent: put_conf_sent,
                            vehicle_conf_signed: put_conf_signed,
                            order_conf_date: put_conf_date,
                            licence_check: put_check_date,
                            contract_end: put_end_date,
                            # new additions since AF req 15/2/22
                            make: put_map_make,
                            model: put_map_model,
                            mileage: put_yearly_id,
                            variant: put_derivative,
                            registration: put_reg_number
                        }

                        response = requests.put(f'https://your-domain.pipedrive.com/api/v1/deals/{i}', params=token, data=csv_data)
                        response.json()
                        if response.ok:
                            updated_deals.append(i)
                            print(i,"was successfully updated!")
                            # see what changed
                            print(i,"used to be - current_stage: ",current_stage,", existing_del_date: ",existing_del_date,", existing_bool_result: ",existing_bool_result,
                            ", existing_agree_date: ",existing_agree_date,", existing_conf_sent: ",existing_conf_sent,", existing_conf_signed: ",existing_conf_signed,
                            ", existing_conf_date: ",existing_conf_date,", existing_check_date: ",existing_check_date,", existing_end_date: ",existing_end_date,
                            ", existing_map_make: ",existing_map_make,", existing_map_model: ",existing_map_model,", existing_yearly_id: ",existing_yearly_id,
                            ", existing_derivative: ",existing_derivative,", existing_reg_number: ",existing_reg_number)
                            print(i,"is now - put_pd_stage: ",put_pd_stage,", put_del_date: ",put_del_date,", put_bool_result: ",put_bool_result,", put_agree_date: ",put_agree_date,
                            ", put_conf_sent: ",put_conf_sent,", put_conf_signed: ",put_conf_signed,", put_conf_date: ",put_conf_date,", put_check_date: ",put_check_date,
                            ", put_end_date: ",put_end_date,", put_map_make: ",put_map_make,", put_map_model: ",put_map_model,", put_yearly_id: ",put_yearly_id,", put_derivative: ",
                            put_derivative,", put_reg_number: ",put_reg_number,"\n")
                        else:
                            print("Something went wrong while trying to update deal number",i,"!")
                            print(i,"used to be - current_stage: ",current_stage,", existing_del_date: ",existing_del_date,", existing_bool_result: ",existing_bool_result,
                            ", existing_agree_date: ",existing_agree_date,", existing_conf_sent: ",existing_conf_sent,", existing_conf_signed: ",existing_conf_signed,
                            ", existing_conf_date: ",existing_conf_date,", existing_check_date: ",existing_check_date,", existing_end_date: ",existing_end_date,
                            ", existing_map_make: ",existing_map_make,", existing_map_model: ",existing_map_model,", existing_yearly_id: ",existing_yearly_id,
                            ", existing_derivative: ",existing_derivative,", existing_reg_number: ",existing_reg_number)
                            print(i,"is now - put_pd_stage: ",put_pd_stage,", put_del_date: ",put_del_date,", put_bool_result: ",put_bool_result,", put_agree_date: ",put_agree_date,
                            ", put_conf_sent: ",put_conf_sent,", put_conf_signed: ",put_conf_signed,", put_conf_date: ",put_conf_date,", put_check_date: ",put_check_date,
                            ", put_end_date: ",put_end_date,", put_map_make: ",put_map_make,", put_map_model: ",put_map_model,", put_yearly_id: ",put_yearly_id,", put_derivative: ",
                            put_derivative,", put_reg_number: ",put_reg_number,"\n")
                            fails.append(i)

                else: # from getting_response.ok
                    unfound_deals.append(i) # deal was not found
                        
        print("Job done!\n")

        # send alert via slack
        slack_token = os.getenv('slack_password')
        slack_channel = '#script-alerts'
        # create func
        def post_message_to_slack(text):
            return requests.post('https://slack.com/api/chat.postMessage', {
                'token': slack_token,
                'channel': slack_channel,
                'text': text,
            }).json()

        if fails:
            # create msg and post to slack
            fails.sort()
            slack_info = f"""Something went wrong while trying to update the following {len(fails)} Deal/s: {fails}. Need to check script on Heroku!
                            Possible issue: a field may have been changed/replaced in Pipedrive? Try checking Pipedrive keys to see if they still match!"""
            post_message_to_slack(slack_info)
            print(slack_info)

        if len(updated_deals) > 0:
            if len(updated_deals) == 1:
                print(f"The following Deal was updated: {updated_deals}\n")
            else:
                updated_deals.sort()
                print(f"The following {len(updated_deals)} Deals were updated: {updated_deals}\n")
        else:
            print("No Deals were updated!\n")
            
        if len(dupes):
                print("The following Deal IDs were duplicates in the Key2 export file, and so were skipped:",dupes,"\n")
        
        if len(unfound_deals) > 0:
            if len(unfound_deals) == 1:
                print(f"The following Deal could not be found in Pipedrive: {unfound_deals}\n")
            else:
                unfound_deals.sort()
                print(f"The following Deals could not be found in Pipedrive: {unfound_deals}\n")
    else:
        # if no file was found
        print("No file was found!")

    ### END OF JOB ###

# run script every hour at 5 mins past the hour (file uploaded on the hour, every hour)
schedule.every().hour.at(":05").do(job)
while True:
    schedule.run_pending()
    time.sleep(1)