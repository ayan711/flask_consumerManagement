import requests,json ,datetime as dt
from flask import Flask,jsonify,request, redirect, url_for, render_template , abort , flash
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine,and_
import sqlalchemy_serializer
from sqlalchemy.orm import sessionmaker
from  flask_marshmallow import Marshmallow
import pretty_errors,xmltodict,sqlite3
import os,csv,pandas as pd,time
import flask_restless,collections
from traceback import print_exc
import random,string,sqlite3
# import flask.ext.restless

app=Flask(__name__)

## Providing configuration details for FLask

app.config['SQLALCHEMY_DATABASE_URI']='sqlite:///complain.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS']=False
app.debug=True

## Creating engine for ORM  
engine = create_engine('sqlite:///D:\Project\\flask-jwt-auth\\complain.db',connect_args={'check_same_thread': False})

## Creating Session object 
Session=sessionmaker(bind=engine)
session=Session()

## Creating connection to DB
connection=engine.connect()


ma=Marshmallow(app)
db=SQLAlchemy(app)




@app.cli.command('db_drop')
def db_drop():
    db.drop_all()
    print('Database dropped!')



class Complain(db.Model):
    """ Defining the schema of the Complain table"""

    __tablename__ = 'Complain'
    ticket=db.Column(db.Integer,primary_key=True)
    issue_date=db.Column(db.String,nullable=True)
    issue_time=db.Column(db.String,nullable=True)
    form=db.Column(db.String(80),nullable=True)
    method=db.Column(db.String(80),nullable=False)
    issue=db.Column(db.String(80),nullable=False)
    caller_ID=db.Column(db.Integer,nullable=True)
    call_message_type=db.Column(db.String(80),nullable=True)
    bNum=db.Column(db.String(80),nullable=False)
    city=db.Column(db.String(80),nullable=False)
    state=db.Column(db.String(80),nullable=False)
    zip=db.Column(db.Integer,nullable=False)
    location=db.Column(db.String(80),nullable=False)


## Serializing the the above table using marshmallow scehma method

class ComplainSchema(ma.Schema):
    class Meta:

        fields=('ticket','issue_date','issue_time','form','method','issue','caller_ID','call_message_type','bNum','city','state','zip','location')


complain_Schema_single=ComplainSchema()
complain_Schema=ComplainSchema(many=True)



@app.route('/load')
def db_loadData():
    """ For Inserting records in table """
    complains = pd.read_csv('customer_Insert.csv')
    # write the data to a sqlite table
    complains.to_sql('Complain', con=connection, if_exists='replace', index = False)
    print(complains)




    return "Table Created"


    # db.session.add_all([test_user1,test_user2])
    # db.session.commit()

@app.route('/create')
def create():

    """ DB table creation with the above defined schema  """

    print(db)
    db.create_all()

    return jsonify("Table Created")

@app.route('/display/<int:page>')
def display(page):

    """ Dispalying records in the table using paginate feature """

    ROWS_PER_PAGE = 5
    #page = request.args.get('page',  1, type = int)
    
    complain = Complain.query.paginate(page = page, per_page =ROWS_PER_PAGE,error_out = True)
    
    print("Complain type -",type(complain))
    print(vars(complain))
    print("Total Records --- ",complain.total)
    paginated_complain =(complain.items)
    print("Paginated complain type - ",type(paginated_complain))

    complain_Count=len(paginated_complain)
    print(complain_Count)

    final=complain_Schema.dump(paginated_complain)

    return jsonify(final)


@app.route('/bulkInsert')
def bulkInsert():
    """ Bulk insertion in the database using csv file  """

    response_dict = collections.defaultdict(list)

    try:
        if request.is_json:
            csvPath = request.json['path']
        else:
            csvPath = request.form['path']
    except Exception as e:
        response_dict["error"]="File path parameter not found"
        return jsonify(response_dict),400

    

    ## Check file present
    print("File",csvPath)

    if len(csvPath) !=0:
    
        if os.path.exists(csvPath):

            #if csvPath.endswith('csv'):
            print("File found")

            
            #response_dict={}
            missingCount=0
            valid_data = []


            with open(str(csvPath), 'r') as read_obj:
                csv_data = csv.DictReader(read_obj)            

                header = csv_data.fieldnames
                print("Fields are", len(header))
                # Check file as empty

                #print("CSV data",list(csv_data))
                csv_data2=list(csv_data)    

                ## checking ticket column in given file
                if 'ticket' in header:
                    if len(header) == 13:

                        for entry in csv_data2:
                            
                            
                            if len(entry['ticket']) != 0:
                                entryCheck=session.query(Complain).filter_by(ticket=entry['ticket']).count()

                                print("Ticket check -", entryCheck)

                                ## Checking if ticket already exist
                                if entryCheck == 1:
                                    response_dict["Existing_Entry"].append(entry['ticket'])
                                    #response_dict["Inserted_Entry"]=entry['ticket']
                                else:
                                    ## checking column name 
                                    try:

                                        newComplain=Complain(ticket=entry['ticket'],issue_date=entry['issue_date'],issue_time=entry['issue_time'],
                                        form=entry['form'],method=entry['method'],issue=entry['issue'],caller_ID=entry['caller_ID'],call_message_type=entry['call_message_type'],
                                        bNum=entry['bNum'],city=entry['city'],state=entry['state'],zip=entry['zip'],location=entry['location'])
                                        print("error passed")
                                        db.session.add(newComplain)
                                        db.session.commit()

                                        

                                        response_dict["Inserted_Entry"].append(entry['ticket'])
                                        #response_dict["Inserted_Entry"]=entry['ticket']

                                        print("Inserted -",entry['ticket'])
                                    except KeyError as e:
                                        
                                        print(e)
                                        session.close()
                                        response_dict["error"]=f"Invalid column name, expecting {e} "
                                        return jsonify(response_dict),400

                                    except Exception as e:
                                        
                                        print(e)
                                        response_dict["error"]=f" Insertion operation failed"
                                        return jsonify(response_dict),500

                                session.close()

                            ## Recording entry which does not have ticket number  
                            else:

                                    missingCount+=1
                                    #response_dict["Missing_TicketID_Count"].append(missingCount)
                                    response_dict["Missing_TicketID_Count"]=missingCount
                            

                    else:
                        response_dict["error"].append("Expected number of columns are not present")
                        return jsonify(response_dict),400

                else:
                    response_dict["error"]="Ticket column not found"

                    return jsonify(response_dict),400

                print("Response - ",response_dict)
                return jsonify(response_dict),201

            # else:
            #     return jsonify("File is not of csv type"),422

        else:
            return jsonify("File not found"),404

    else:
        response_dict["error"]="File path was not given"
        return jsonify(response_dict),400
        
@app.route('/findComplain')
def findComplain():
    """ Filtering the table by various optional parameters and a compulsory parameter "Issue" """
    form = request.json


    insensitiveFilters=[]

    ## check if issue (mandatory) argument present 
    if "issue" in form:
        for col in form:

            try:
                #### Case Insensitive Filtering ####
                formValues_Exp=(getattr(Complain, col).ilike('%'+form[col]+'%'))
                print("Values -",formValues_Exp)

                insensitiveFilters.append(formValues_Exp)

                #### Sensitive filtering ####

                # print("Get attr -",getattr(Complain, col))
                # sensitiveExpression = (getattr(Complain, col) == form[col])
                
                # filters.append(sensitiveExpression)


            except Exception as e:

                print("Invalid Parameter")

        print("Insensitive -",insensitiveFilters)


        

        #filteredRecords = session.query(Complain).where(and_(*filters)).all() 
        filteredRecords = session.query(Complain).where(and_(*insensitiveFilters)).all() 
        session.close()
        

        ## serializing the fiiltered records
        final=complain_Schema.dump(filteredRecords)




        return jsonify({"Count":len(filteredRecords),"Data":final}),200
    
    else:
        return jsonify({"error":"issue filter is mandatory"}),400



if __name__ == "__main__":
    app.run(debug=True)

