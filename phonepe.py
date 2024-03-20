
import psycopg2
import pandas as pd
import streamlit as st 
from streamlit_option_menu import option_menu
import plotly
import plotly.express as px
import requests
import json
from PIL import Image


#SQL Connection
postgres_connection = psycopg2.connect(host = 'localhost',
                                       user = 'postgres',
                                       password = 'sudhakar',
                                       database = 'phonepe_data',
                                       port = 5432)
postgres_cursor = postgres_connection.cursor()


# Aggregate insurance dataframe
postgres_cursor.execute('SELECT * from aggregated_insurance')
postgres_connection.commit()
aggregated_insurance_data = postgres_cursor.fetchall()
AGGRE_INSURANCE = pd.DataFrame(aggregated_insurance_data, columns = ('States', 'Years', 'Quarters', 'Transaction_type', 
                                                                     'Transaction_count', 'Transaction_amount'))

# Aggregate transaction dataframe
postgres_cursor.execute('SELECT * from aggregated_transaction')
postgres_connection.commit()
aggregated_transaction_data = postgres_cursor.fetchall()
AGGRE_TRANSACTION = pd.DataFrame(aggregated_transaction_data, columns = ('States', 'Years', 'Quarters', 'Transaction_type', 
                                                                     'Transaction_count', 'Transaction_amount'))

# Aggregate user dataframe
postgres_cursor.execute('SELECT * from aggregated_user')
postgres_connection.commit()
aggregated_user_data = postgres_cursor.fetchall()
AGGRE_USER = pd.DataFrame(aggregated_user_data, columns = ('States', 'Years', 'Quarters', 'Brands', 'Transaction_count', 'Percentage'))

# Map insurance dataframe
postgres_cursor.execute('SELECT * from map_insurance')
postgres_connection.commit()
map_insurance_data = postgres_cursor.fetchall()
MAP_INSURANCE = pd.DataFrame(map_insurance_data, columns = ('States', 'Years', 'Quarters', 'District', 
                                                            'Transaction_count', 'Transaction_amount'))

# Map transaction dataframe
postgres_cursor.execute('SELECT * from map_transaction')
postgres_connection.commit()
map_transaction_data = postgres_cursor.fetchall()
MAP_TRANSACTION = pd.DataFrame(map_transaction_data, columns = ('States', 'Years', 'Quarters', 'District', 
                                                            'Transaction_count', 'Transaction_amount'))

# Map user dataframe
postgres_cursor.execute('SELECT * from map_user')
postgres_connection.commit()
map_user_data = postgres_cursor.fetchall()
MAP_USER = pd.DataFrame(map_user_data, columns = ('States', 'Years', 'Quarters', 'District', 
                                                  'RegisteredUsers_count', 'AppOpens_count'))

# Top insurance dataframe
postgres_cursor.execute('SELECT * from top_insurance')
postgres_connection.commit()
top_insurance_data = postgres_cursor.fetchall()
TOP_INSURANCE = pd.DataFrame(top_insurance_data, columns = ('States', 'Years', 'Quarters', 'Pincode', 'Transaction_count', 'Transaction_amount'))


# Top transaction dataframe
postgres_cursor.execute('SELECT * from top_transaction')
postgres_connection.commit()
top_transaction_data = postgres_cursor.fetchall()
TOP_TRANSACTION = pd.DataFrame(top_transaction_data, columns = ('States', 'Years', 'Quarters', 'Pincode', 'Transaction_count', 'Transaction_amount'))


# Top user dataframe
postgres_cursor.execute('SELECT * from top_user')
postgres_connection.commit()
top_user_data = postgres_cursor.fetchall()
TOP_USER = pd.DataFrame(top_user_data, columns = ('States', 'Years', 'Quarters', 'Pincode', 'RegisteredUsers'))


#Transaction year based
def Transaction_of_Amount_Count_Year(df, year):
    Tra_Amo_Cou_Year = df[df['Years'] == year]
    Tra_Amo_Cou_Year.reset_index(drop= True, inplace= True)

    Tra_Amo_Cou_Year_Group = Tra_Amo_Cou_Year.groupby('States')[['Transaction_count', 'Transaction_amount']].sum()
    Tra_Amo_Cou_Year_Group.reset_index(inplace= True)
    column1, column2 = st.columns(2)
    with column1: 
        Figure_Amt_bar = px.bar(Tra_Amo_Cou_Year_Group, x= 'States', y= 'Transaction_amount', title= f'{year} TRANSACTION AMOUNT',
                            color_discrete_sequence= px.colors.sequential.Purples_r)
        st.plotly_chart(Figure_Amt_bar)
    with column2:
        Figure_Count_bar = px.bar(Tra_Amo_Cou_Year_Group, x= 'States', y= 'Transaction_count', title= f'{year} TRANSACTION COUNT',
                            color_discrete_sequence= px.colors.sequential.Rainbow)
        st.plotly_chart(Figure_Count_bar)
    
    url = 'https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson'
    response = requests.get(url)
    world_data1 = json.loads(response.content)
    States_name = []
    for i in world_data1['features']:
        States_name.append(i['properties']['ST_NM'])
    States_name.sort()
    column1, column2 = st.columns(2)
    with column1:
        India_figure_amt = px.choropleth(Tra_Amo_Cou_Year_Group, geojson=world_data1, locations='States', featureidkey='properties.ST_NM',
                                        color='Transaction_amount', color_continuous_scale='rainbow', 
                                        range_color=(Tra_Amo_Cou_Year_Group['Transaction_amount'].min(), Tra_Amo_Cou_Year_Group['Transaction_amount'].max()), # Corrected line
                                        hover_name='States', title=f'{year} TRANSACTION AMOUNT', fitbounds='locations',
                                        height=600, width=600)
        India_figure_amt.update_geos(visible = False)
        st.plotly_chart(India_figure_amt)
    with column2:
        India_figure_count = px.choropleth(Tra_Amo_Cou_Year_Group, geojson=world_data1, locations='States', featureidkey='properties.ST_NM',
                                        color='Transaction_count', color_continuous_scale='rainbow', 
                                        range_color=(Tra_Amo_Cou_Year_Group['Transaction_count'].min(), Tra_Amo_Cou_Year_Group['Transaction_count'].max()), # Corrected line
                                        hover_name='States', title=f'{year} TRANSACTION COUNT', fitbounds='locations',
                                        height=600, width=600)
        India_figure_count.update_geos(visible = False)
        st.plotly_chart(India_figure_count)
    
    return Tra_Amo_Cou_Year
    
 
#Transaction quarter based        
def Transaction_of_Amount_Count_Year_Quarter(df, Quarter):
    Tra_Amo_Cou_Year_Quarter = df[df['Quarters'] == Quarter]
    Tra_Amo_Cou_Year_Quarter.reset_index(drop= True, inplace= True)

    Tra_Amo_Cou_Year_Quarter_Group = Tra_Amo_Cou_Year_Quarter.groupby('States')[['Transaction_count', 'Transaction_amount']].sum()
    Tra_Amo_Cou_Year_Quarter_Group.reset_index(inplace= True)

    column1, column2 = st.columns(2)
    with column1:     
        Figure_Amt = px.bar(Tra_Amo_Cou_Year_Quarter_Group, x= 'States', y= 'Transaction_amount', title= f"{Tra_Amo_Cou_Year_Quarter['Years'].min()} Year {Quarter} Quarter TRANSACTION AMOUNT",
                            color_discrete_sequence= px.colors.sequential.Purples_r)
        st.plotly_chart(Figure_Amt)
        
    with column2:
        Figure_Count = px.bar(Tra_Amo_Cou_Year_Quarter_Group, x= 'States', y= 'Transaction_count', title= f"{Tra_Amo_Cou_Year_Quarter['Years'].min()} Year {Quarter} TRANSACTION COUNT",
                            color_discrete_sequence= px.colors.sequential.Rainbow)
        st.plotly_chart(Figure_Count)
    
    url = 'https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson'
    response = requests.get(url)
    world_data1 = json.loads(response.content)
    States_name = []
    for i in world_data1['features']:
        States_name.append(i['properties']['ST_NM'])
    States_name.sort()

    column1, column2 = st.columns(2)
    with column1:
        India_figure_amt = px.choropleth(Tra_Amo_Cou_Year_Quarter_Group, geojson=world_data1, locations='States', featureidkey='properties.ST_NM',
                                        color='Transaction_amount', color_continuous_scale='rainbow', 
                                        range_color=(Tra_Amo_Cou_Year_Quarter_Group['Transaction_amount'].min(), Tra_Amo_Cou_Year_Quarter_Group['Transaction_amount'].max()), # Corrected line
                                        hover_name='States', title=f"{Tra_Amo_Cou_Year_Quarter['Years'].min()} Year {Quarter} TRANSACTION AMOUNT", fitbounds='locations',
                                        height=600, width=600)
        India_figure_amt.update_geos(visible = False)
        st.plotly_chart(India_figure_amt)
    
    with column2:
        India_figure_count = px.choropleth(Tra_Amo_Cou_Year_Quarter_Group, geojson=world_data1, locations='States', featureidkey='properties.ST_NM',
                                        color='Transaction_count', color_continuous_scale='rainbow', 
                                        range_color=(Tra_Amo_Cou_Year_Quarter_Group['Transaction_count'].min(), Tra_Amo_Cou_Year_Quarter_Group['Transaction_count'].max()), # Corrected line
                                        hover_name='States', title=f"{Tra_Amo_Cou_Year_Quarter['Years'].min()} Year {Quarter} TRANSACTION COUNT", fitbounds='locations',
                                        height=600, width=600)
        India_figure_count.update_geos(visible = False)
        st.plotly_chart(India_figure_count) 
        
    return Tra_Amo_Cou_Year_Quarter
        

#Transaction Type based
def Aggre_Trans_Transaction_Type(df, state):
    Tra_Amo_Cou_Year_Type = df[df['States'] == state]
    Tra_Amo_Cou_Year_Type.reset_index(drop= True, inplace= True)

    Tra_Amo_Cou_Year_Type_Group = Tra_Amo_Cou_Year_Type.groupby('Transaction_type')[['Transaction_count', 'Transaction_amount']].sum()
    Tra_Amo_Cou_Year_Type_Group.reset_index(inplace= True)
    
    column1, column2 = st.columns(2)
    with column1:
        Figure_Amt_pie = px.pie(data_frame= Tra_Amo_Cou_Year_Type_Group, names= 'Transaction_type', values= 'Transaction_amount',
                            width= 800, title= f'{state.upper()} TRANSACTION AMOUNT', hole= 0.2)
        st.plotly_chart(Figure_Amt_pie)
        
    with column2:
        Figure_Count_pie = px.pie(data_frame= Tra_Amo_Cou_Year_Type_Group, names= 'Transaction_type', values= 'Transaction_count',
                                width= 800, title= f'{state.upper()} TRANSACTION COUNT', hole= 0.2)
        st.plotly_chart(Figure_Count_pie)
        
        
# Aggre user year wise
def Aggre_User_Year_Wise_Plot(df, year):
    Aggre_User_Year = df[df['Years'] == year]
    Aggre_User_Year.reset_index(drop= True, inplace= True)

    Aggre_User_Year_Group =pd.DataFrame(Aggre_User_Year.groupby('Brands')['Count'].sum())
    Aggre_User_Year_Group.reset_index(inplace=True)

    Figure_Count_bar = px.bar(Aggre_User_Year_Group, x = 'Brands', y = 'Count', title= 'BRANDS AND TRANSACTION COUNT',
                            width= 800, color_discrete_sequence= px.colors.sequential.Pinkyl_r)
    st.plotly_chart(Figure_Count_bar)
    
    return Aggre_User_Year


# Aggre user quarter wise
def Aggre_User_Quarter_Wise_Plot(df, Quarter):
    Aggre_User_Year_Quarter = df[df['Quarters'] == Quarter]
    Aggre_User_Year_Quarter.reset_index(drop= True, inplace= True)

    Aggre_User_Year_Quarter_Group = pd.DataFrame(Aggre_User_Year_Quarter.groupby('Brands')['Count'].sum())
    Aggre_User_Year_Quarter_Group.reset_index(inplace= True)

    Figure_count_bar = px.bar(Aggre_User_Year_Quarter_Group, x = 'Brands', y = 'Count', title= f'{Quarter} Quarter BRANDS AND TRANSACTION COUNT',
                            width= 800, color_discrete_sequence= px.colors.sequential.Pinkyl_r)
    st.plotly_chart(Figure_count_bar)

    return Aggre_User_Year_Quarter


# Aggre user state wise
def Aggre_User_State_Wise_Plot(df, state):
    Aggre_User_Y_Q_S = df[df['States'] == state]
    Aggre_User_Y_Q_S.reset_index(drop= True, inplace= True)

    Figure_count_line = px.line(Aggre_User_Y_Q_S, x = 'Brands', y = 'Count', hover_data= 'Percentage',
                                title= 'BRANDS, TRANSACTION COUNT AND PERCENTAGE', width= 800, markers= True)
                            
    st.plotly_chart(Figure_count_line)
    
      
# Map Insurance District based
def Map_Insurance_District(df, state):
    Tra_Amo_Cou_Year_District = df[df['States'] == state]
    Tra_Amo_Cou_Year_District.reset_index(drop= True, inplace= True)

    Tra_Amo_Cou_Year_District_Group = Tra_Amo_Cou_Year_District.groupby('District')[['Transaction_count', 'Transaction_amount']].sum()
    Tra_Amo_Cou_Year_District_Group.reset_index(inplace= True)
    
    column1, column2 = st.columns(2)
    with column1:
        Figure_Amt_bar = px.bar(Tra_Amo_Cou_Year_District_Group, x= 'District', y= 'Transaction_amount',title= f'{state.upper()} DISTRICT AND TRANSACTION AMOUNT')                      
        st.plotly_chart(Figure_Amt_bar)
        
    with column2:
        Figure_Count_bar = px.bar(Tra_Amo_Cou_Year_District_Group, x= 'District', y= 'Transaction_count',
                                title= f'{state.upper()} DISTRICT AND TRANSACTION COUNT', color_discrete_sequence=px.colors.sequential.Magenta_r)                       
        st.plotly_chart(Figure_Count_bar)
        
        
# map user year wise
def Map_User_Year_Wise_Plot(df, year):
    Map_User_Year = df[df['Years'] == year]
    Map_User_Year.reset_index(drop= True, inplace= True)

    Map_User_Year_Group =pd.DataFrame(Map_User_Year.groupby('States')[['RegisteredUsers_count', 'AppOpens_count']].sum())
    Map_User_Year_Group.reset_index(inplace=True)

    Figure_count_line = px.line(Map_User_Year_Group, x = 'States', y = ['RegisteredUsers_count', 'AppOpens_count'],
                                    title= f'{year} REGISTERED USER COUNT AND APP OPENS COUNT', width= 1000, height= 700, markers= True)
    st.plotly_chart(Figure_count_line)   
    
    return Map_User_Year


# Map user quarter wise
def Map_User_Quarter_Wise_Plot(df, quarter):
    Map_User_Year_Quarter = df[df['Quarters'] == quarter]
    Map_User_Year_Quarter.reset_index(drop= True, inplace= True)

    Map_User_Year_Quarter_Group = pd.DataFrame(Map_User_Year_Quarter.groupby('States')[['RegisteredUsers_count', 'AppOpens_count']].sum())
    Map_User_Year_Quarter_Group.reset_index(inplace= True)

    Figure_count_line = px.line(Map_User_Year_Quarter_Group, x = 'States', y = ['RegisteredUsers_count', 'AppOpens_count'],
                                        title= f"{df['Years'].min()} YEAR {quarter} REGISTERED USER COUNT AND APP OPENS COUNT", width= 1000, height= 700, markers= True)
    st.plotly_chart(Figure_count_line)    

    return Map_User_Year_Quarter
    
    
    # Map user state wise
def Map_User_State_Wise_Plot(df, state):
    Map_User_Year_Quarter_State = df[df['States'] == state]
    Map_User_Year_Quarter_State.reset_index(drop= True, inplace= True)

    column1, column2 = st.columns(2)
    with column1:
        Figure_MapUser_bar_1 = px.bar(Map_User_Year_Quarter_State, x= 'District', y= 'RegisteredUsers_count', title= f'{state.upper()} REGISTERED USER COUNT',
                                    color_discrete_sequence= px.colors.sequential.Rainbow_r, width= 800, height= 600)
        st.plotly_chart(Figure_MapUser_bar_1)

    with column2:
        Figure_MapUser_bar_2 = px.bar(Map_User_Year_Quarter_State, x= 'District', y= 'AppOpens_count', 
                                      title= f'{state.upper()} APP OPENS COUNT')                            
        st.plotly_chart(Figure_MapUser_bar_2)
        
        
# Top Insurance Quarters Pincode wise
def Top_Insurance_Quarters_Pincode_Wise_Plot(df, states):
    Top_Insurance_Year = df[df['States'] == states]
    Top_Insurance_Year.reset_index(drop= True, inplace= True)

    column1, column2 = st.columns(2)
    with column1:
        Figure_Top_Ins_bar_1 = px.bar(Top_Insurance_Year, x= 'Quarters', y= 'Transaction_count', hover_data= 'Pincode',
                                    title= 'PINCODE WITH TRANSACTION COUNT', color_discrete_sequence= px.colors.sequential.Rainbow_r)
        st.plotly_chart(Figure_Top_Ins_bar_1)
    
    with column2:
        Figure_Top_Ins_bar_2 = px.bar(Top_Insurance_Year, x= 'Quarters', y= 'Transaction_amount', hover_data= 'Pincode',
                                    title= 'PINCODE WITH TRANSACTION COUNT', color_discrete_sequence= px.colors.sequential.Rainbow)
        st.plotly_chart(Figure_Top_Ins_bar_2)


# Top user year wise
def Top_User_Year_Quarters_Wise_Plot(df, year):
    Top_User_Year = df[df['Years'] == year]
    Top_User_Year.reset_index(drop= True, inplace= True)

    Top_User_Year_Quarter_Group =pd.DataFrame(Top_User_Year.groupby(['States', 'Quarters'])['RegisteredUsers'].sum())
    Top_User_Year_Quarter_Group.reset_index(inplace=True)

    Figure_Top_User_bar_1 = px.bar(Top_User_Year_Quarter_Group, x= 'States', y= 'RegisteredUsers', color= 'Quarters', 
                                   title= f'{year} REGISTER USER COUNT', color_discrete_sequence= px.colors.sequential.Burgyl_r, height= 600)
    st.plotly_chart(Figure_Top_User_bar_1)
    
    return Top_User_Year   


# Top user States Pincode Quarter wise
def Top_User_States_Wise_Plot(df, state):
    Top_User_Year_States = df[df['States'] == state]
    Top_User_Year_States.reset_index(drop= True, inplace= True)

    Figure_Top_User_bar_2 = px.bar(Top_User_Year_States, x= 'Quarters', y= 'RegisteredUsers', color= 'RegisteredUsers', hover_data= 'Pincode',
                                    title= f'{state} REGISTER USER COUNT, QUARTER AND PINCODE', color_continuous_scale= px.colors.sequential.Magenta)
    st.plotly_chart(Figure_Top_User_bar_2)



def Top_Chart_Transaction_Amount(Table_Name):
    # Plot_1
    Query_1 = f'''select States, sum(Transaction_amount) as Transaction_amount
                    from {Table_Name} 
                    group by States
                    order by Transaction_amount desc
                    limit 10'''
                
    postgres_cursor.execute(Query_1)
    Table_1 = postgres_cursor.fetchall()
    postgres_connection.commit()

    column1, column2 = st.columns(2)
    with column1:
        Df_1 = pd.DataFrame(Table_1, columns= ('States','Transaction_amount'))
        Figure_Amt_1 = px.bar(Df_1, x= 'States', y= 'Transaction_amount', title= 'TOP 10 TRANSACTION AMOUNT',
                                color_discrete_sequence= px.colors.sequential.Purples_r)
        st.plotly_chart(Figure_Amt_1)

    # Plot_2
    Query_2 = f'''select States, sum(Transaction_amount) as Transaction_amount
                    from {Table_Name} 
                    group by States
                    order by Transaction_amount
                    limit 10'''
                
    postgres_cursor.execute(Query_2)
    Table_2 = postgres_cursor.fetchall()
    postgres_connection.commit()

    with column2:
        Df_2 = pd.DataFrame(Table_2, columns= ('States','Transaction_amount'))
        Figure_Amt_2 = px.bar(Df_2, x= 'States', y= 'Transaction_amount', title= 'LEAST 10 TRANSACTION AMOUNT')
        st.plotly_chart(Figure_Amt_2)

    # Plot_3
    Query_3 = f'''select States, avg(Transaction_amount) as Transaction_amount
                    from {Table_Name} 
                    group by States
                    order by Transaction_amount'''
                
    postgres_cursor.execute(Query_3)
    Table_3 = postgres_cursor.fetchall()
    postgres_connection.commit()

    Df_3 = pd.DataFrame(Table_3, columns= ('States','Transaction_amount'))
    Figure_Amt_3 = px.bar(Df_3, x= 'States', y= 'Transaction_amount', title= 'AVERAGE TRANSACTION AMOUNT', height= 800, width= 1000)
    st.plotly_chart(Figure_Amt_3)
    
    
    
def Top_Chart_Transaction_Count(Table_Name):
    # Plot_1
    Query_1 = f'''select States, sum(Transaction_count) as Transaction_count
                    from {Table_Name} 
                    group by States
                    order by Transaction_count desc
                    limit 10'''
                
    postgres_cursor.execute(Query_1)
    Table_1 = postgres_cursor.fetchall()
    postgres_connection.commit()

    column1, column2 = st.columns(2)
    with column1:
        Df_1 = pd.DataFrame(Table_1, columns= ('States','Transaction_count'))
        Figure_Count_1 = px.bar(Df_1, x= 'States', y= 'Transaction_count', title= 'TOP 10 TRANSACTION COUNT',
                                color_discrete_sequence= px.colors.sequential.Purples_r)
        st.plotly_chart(Figure_Count_1)

    # Plot_2
    Query_2 = f'''select States, sum(Transaction_count) as Transaction_count
                    from {Table_Name} 
                    group by States
                    order by Transaction_count
                    limit 10'''
                
    postgres_cursor.execute(Query_2)
    Table_2 = postgres_cursor.fetchall()
    postgres_connection.commit()

    with column2:
        Df_2 = pd.DataFrame(Table_2, columns= ('States','Transaction_count'))
        Figure_Count_2 = px.bar(Df_2, x= 'States', y= 'Transaction_count', title= 'LEAST 10 TRANSACTION COUNT')
        st.plotly_chart(Figure_Count_2)

    # Plot_3
    Query_3 = f'''select States, avg(Transaction_count) as Transaction_count
                    from {Table_Name} 
                    group by States
                    order by Transaction_count'''
                
    postgres_cursor.execute(Query_3)
    Table_3 = postgres_cursor.fetchall()
    postgres_connection.commit()

    Df_3 = pd.DataFrame(Table_3, columns= ('States','Transaction_count'))
    Figure_Count_3 = px.bar(Df_3, x= 'States', y= 'Transaction_count', title= 'AVERAGE TRANSACTION COUNT', height= 800)
    st.plotly_chart(Figure_Count_3)



def Top_Chart_Transaction_RegisteredUsers_count(Table_Name, State):
    # Plot_1
    Query_1 = f'''select District, sum(RegisteredUsers_count) as RegisteredUsers_count 
                    from {Table_Name}  where States = '{State}'
                    group by District order by RegisteredUsers_count desc limit 10'''
                
    postgres_cursor.execute(Query_1)
    Table_1 = postgres_cursor.fetchall()
    postgres_connection.commit()

    column1, column2 = st.columns(2)
    with column1:
        Df_1 = pd.DataFrame(Table_1, columns= ('District','RegisteredUsers_count'))
        Figure_Reg_User_1 = px.bar(Df_1, x= 'District', y= 'RegisteredUsers_count', title= 'TOP 10 REGISTERED USER',
                                color_discrete_sequence= px.colors.sequential.Purples_r)
        st.plotly_chart(Figure_Reg_User_1)

    # Plot_2
    Query_2 = f'''select District, sum(RegisteredUsers_count) as RegisteredUsers_count 
                   from {Table_Name}  where States = '{State}'
                    group by District order by RegisteredUsers_count asc limit 10'''
                
    postgres_cursor.execute(Query_2)
    Table_2 = postgres_cursor.fetchall()
    postgres_connection.commit()

    with column2:
        Df_2 = pd.DataFrame(Table_2, columns= ('District','RegisteredUsers_count'))
        Figure_Reg_User_2 = px.bar(Df_2, x= 'District', y= 'RegisteredUsers_count', title= 'LEAST 10 REGISTERED USER')
        st.plotly_chart(Figure_Reg_User_2)

    # Plot_3
    Query_3 = f'''select District, avg(RegisteredUsers_count) as RegisteredUsers_count 
                    from {Table_Name}  where States = '{State}'
                    group by District order by RegisteredUsers_count desc '''
                
    postgres_cursor.execute(Query_3)
    Table_3 = postgres_cursor.fetchall()
    postgres_connection.commit()

    Df_3 = pd.DataFrame(Table_3, columns= ('District','RegisteredUsers_count'))
    Figure_Reg_User_3 = px.bar(Df_3, x= 'District', y= 'RegisteredUsers_count', title= 'AVERAGE REGISTERED USER', height= 800)
    st.plotly_chart(Figure_Reg_User_3)



def Top_Chart_Transaction_AppOpens_count(Table_Name, State):
    # Plot_1
    Query_1 = f'''select District, sum(AppOpens_count) as AppOpens_count 
                    from {Table_Name}  where States = '{State}'
                    group by District order by AppOpens_count desc limit 10'''
                
    postgres_cursor.execute(Query_1)
    Table_1 = postgres_cursor.fetchall()
    postgres_connection.commit()

    column1, column2 = st.columns(2)
    with column1:
        Df_1 = pd.DataFrame(Table_1, columns= ('District','AppOpens_count'))
        Figure_App_opens_Count_1 = px.bar(Df_1, x= 'District', y= 'AppOpens_count', title= 'TOP 10 APP OPENS COUNT',
                                color_discrete_sequence= px.colors.sequential.Purples_r)
        st.plotly_chart(Figure_App_opens_Count_1)

    # Plot_2
    Query_2 = f'''select District, sum(AppOpens_count) as AppOpens_count 
                   from {Table_Name}  where States = '{State}'
                    group by District order by AppOpens_count asc limit 10'''
                
    postgres_cursor.execute(Query_2)
    Table_2 = postgres_cursor.fetchall()
    postgres_connection.commit()

    with column2:
        Df_2 = pd.DataFrame(Table_2, columns= ('District','AppOpens_count'))
        Figure_App_opens_Count_2 = px.bar(Df_2, x= 'District', y= 'AppOpens_count', title= 'LEAST 10 APP OPENS COUNT')
        st.plotly_chart(Figure_App_opens_Count_2)

    # Plot_3
    Query_3 = f'''select District, avg(AppOpens_count) as AppOpens_count 
                    from {Table_Name}  where States = '{State}'
                    group by District order by AppOpens_count desc '''
                
    postgres_cursor.execute(Query_3)
    Table_3 = postgres_cursor.fetchall()
    postgres_connection.commit()

    Df_3 = pd.DataFrame(Table_3, columns= ('District','AppOpens_count'))
    Figure_App_opens_Count_3 = px.bar(Df_3, x= 'District', y= 'AppOpens_count', title= 'AVERAGE APP OPENS COUNT',height= 600, width= 1000)
    st.plotly_chart(Figure_App_opens_Count_3)



def Top_Chart_RegisteredUsers_count(Table_Name):
    # Plot_1
    Query_1 = f'''select States, sum(RegisteredUsers) as RegisteredUsers 
                    from {Table_Name}
                    group by states order by RegisteredUsers desc limit 10'''
                
    postgres_cursor.execute(Query_1)
    Table_1 = postgres_cursor.fetchall()
    postgres_connection.commit()

    column1, column2 = st.columns(2)
    with column1:
        Df_1 = pd.DataFrame(Table_1, columns= ('States','RegisteredUsers'))
        Figure_Reg_User_1 = px.bar(Df_1, x= 'States', y= 'RegisteredUsers', title= 'TOP 10 REGISTERED USER',
                                color_discrete_sequence= px.colors.sequential.Purples_r)
        st.plotly_chart(Figure_Reg_User_1)

    # Plot_2
    Query_2 = f'''select States, sum(RegisteredUsers) as RegisteredUsers 
                    from {Table_Name}
                    group by states order by RegisteredUsers asc limit 10'''
                
    postgres_cursor.execute(Query_2)
    Table_2 = postgres_cursor.fetchall()
    postgres_connection.commit()

    with column2:
        Df_2 = pd.DataFrame(Table_2, columns= ('States','RegisteredUsers'))
        Figure_Reg_User_2 = px.bar(Df_2, x= 'States', y= 'RegisteredUsers', title= 'LEAST 10 REGISTERED USER')
        st.plotly_chart(Figure_Reg_User_2)

    # Plot_3
    Query_3 = f'''select States, avg(RegisteredUsers) as RegisteredUsers 
                    from {Table_Name}
                    group by states order by RegisteredUsers asc'''
                
    postgres_cursor.execute(Query_3)
    Table_3 = postgres_cursor.fetchall()
    postgres_connection.commit()

    Df_3 = pd.DataFrame(Table_3, columns= ('States','RegisteredUsers'))
    Figure_Reg_User_3 = px.bar(Df_3, x= 'States', y= 'RegisteredUsers', title= 'AVERAGE REGISTERED USER', height= 800)
    st.plotly_chart(Figure_Reg_User_3)




# streamlit view
st.set_page_config(layout= 'wide')
st.title('PHONEPE PULSE DATA VISUALIZATION AND EXPLORATION')

with st.sidebar:
    select = option_menu('Main Menu',['Home', 'Data Exploration', 'Top Charts'])
    
if select == 'Home':
    
    column1, column2 = st.columns(2)
    with column1:
        st.header("PHONEPE")
        st.subheader("INDIA'S BEST TRANSACTION APP")
        st.markdown("PhonePe  is an Indian digital payments and financial technology company")
        st.write("****FEATURES :****")
        st.write("****Credit & Debit card linking****")
        st.write("****Bank Balance check****")
        st.write("****Money Storage****")
        st.write("****PIN Authorization****")
        st.download_button("DOWNLOAD THE APP NOW", "https://www.phonepe.com/app-download/")
    with column2:
        st.image(Image.open(r"C:\Users\Windows\Desktop\DATA SCIENCE project\PhonePay\Phope image.png"), width= 700)

elif select == 'Data Exploration':
    tab1, tab2, tab3 = st.tabs(['Aggregated analysis', 'Map analysis', 'Top analysis'])
    
    with tab1 :
        method = st.radio('Select the method',['Aggregated Insurance', 'Aggregated Transaction', 'Aggregated User'])
        
        if method == 'Aggregated Insurance':
            column1, column2 = st.columns(2)
            with column1:
                years = st.slider('Select the year',AGGRE_INSURANCE['Years'].min(), 
                                  AGGRE_INSURANCE['Years'].max(), AGGRE_INSURANCE['Years'].min())
            T_A_C_Y = Transaction_of_Amount_Count_Year(AGGRE_INSURANCE, years)
            
            column1, column2 = st.columns(2)
            with column1:
                Quarters = st.slider('Select the Quarter',T_A_C_Y['Quarters'].min(), 
                                  T_A_C_Y['Quarters'].max(), T_A_C_Y['Quarters'].min())
            Transaction_of_Amount_Count_Year_Quarter(T_A_C_Y, Quarters)
            
            
        elif method == 'Aggregated Transaction':
            column1, column2 = st.columns(2)
            with column1:
                years = st.slider('Select the year',AGGRE_TRANSACTION['Years'].min(), 
                                  AGGRE_TRANSACTION['Years'].max(), AGGRE_TRANSACTION['Years'].min())
            Aggre_Trans_T_A_C_Y = Transaction_of_Amount_Count_Year(AGGRE_TRANSACTION, years)
            
            column1, column2 = st.columns(2)
            with column1:
                States = st.selectbox('Select the State', Aggre_Trans_T_A_C_Y['States'].unique())
            Aggre_Trans_Transaction_Type(Aggre_Trans_T_A_C_Y, States)
            
            column1, column2 = st.columns(2)
            with column1:
                Quarters = st.slider('Select the Quarter',Aggre_Trans_T_A_C_Y['Quarters'].min(), 
                                  Aggre_Trans_T_A_C_Y['Quarters'].max(), Aggre_Trans_T_A_C_Y['Quarters'].min())
            Aggre_Trans_T_A_C_Y_Q = Transaction_of_Amount_Count_Year_Quarter(Aggre_Trans_T_A_C_Y, Quarters)
            
            column1, column2 = st.columns(2)
            with column1:
                States = st.selectbox('Select the State for pie chart view', Aggre_Trans_T_A_C_Y_Q['States'].unique())
            Aggre_Trans_Transaction_Type(Aggre_Trans_T_A_C_Y_Q, States)
               
               
        elif method == 'Aggregated User':
            column1, column2 = st.columns(2)
            with column1:
                years = st.slider('Select the year',AGGRE_USER['Years'].min(), 
                                  AGGRE_USER['Years'].max(), AGGRE_USER['Years'].min())
            Aggre_User_Year = Aggre_User_Year_Wise_Plot(AGGRE_USER, years)
            
            column1, column2 = st.columns(2)
            with column1:
                quarters = st.slider('Select the quarter',Aggre_User_Year['Quarters'].min(), 
                                  Aggre_User_Year['Quarters'].max(), Aggre_User_Year['Quarters'].min())
                Aggre_User_Year_Quarter = Aggre_User_Quarter_Wise_Plot(Aggre_User_Year, quarters)
                
            column1, column2 = st.columns(2)
            with column1:
                States = st.selectbox('Select the State', Aggre_User_Year_Quarter['States'].unique())
            Aggre_User_State_Wise_Plot(Aggre_User_Year_Quarter, States)
            
            
        
    with tab2 : 
        method2 = st.radio('Select the method',['Map Insurance', 'Map Transaction', 'Map User'])
        
        if method2 == 'Map Insurance':
            column1, column2 = st.columns(2)
            with column1:
                years = st.slider('Select the year for MAP Insurance',MAP_INSURANCE['Years'].min(), 
                                  MAP_INSURANCE['Years'].max(), AGGRE_INSURANCE['Years'].min())
            Map_insurance_T_A_C_Y = Transaction_of_Amount_Count_Year(MAP_INSURANCE, years)
            
            column1, column2 = st.columns(2)
            with column1:
                States = st.selectbox('Select the State for MAP Insurance', Map_insurance_T_A_C_Y['States'].unique())
            Map_Insurance_District(Map_insurance_T_A_C_Y, States)
            
            column1, column2 = st.columns(2)
            with column1:
                Quarters = st.slider('Select the Quarters for MAP Insurance',Map_insurance_T_A_C_Y['Quarters'].min(), 
                                  Map_insurance_T_A_C_Y['Quarters'].max(), Map_insurance_T_A_C_Y['Quarters'].min())
            Map_insurance_T_A_C_Y_Q = Transaction_of_Amount_Count_Year_Quarter(Map_insurance_T_A_C_Y, Quarters)
            
            column1, column2 = st.columns(2)
            with column1:
                States = st.selectbox('Select the State for bar chart view', Map_insurance_T_A_C_Y_Q['States'].unique())
            Map_Insurance_District(Map_insurance_T_A_C_Y_Q, States)
            
        
        elif method2 == 'Map Transaction':
            column1, column2 = st.columns(2)
            with column1:
                years = st.slider('Select the year',MAP_TRANSACTION['Years'].min(), 
                                  MAP_TRANSACTION['Years'].max(), MAP_TRANSACTION['Years'].min())
            Map_transaction_T_A_C_Y = Transaction_of_Amount_Count_Year(MAP_TRANSACTION, years)
            
            column1, column2 = st.columns(2)
            with column1:
                States = st.selectbox('Select the State for MAP transaction', Map_transaction_T_A_C_Y['States'].unique())
            Map_Insurance_District(Map_transaction_T_A_C_Y, States)
            
            column1, column2 = st.columns(2)
            with column1:
                Quarters = st.slider('Select the Quarter',Map_transaction_T_A_C_Y['Quarters'].min(), 
                                  Map_transaction_T_A_C_Y['Quarters'].max(), Map_transaction_T_A_C_Y['Quarters'].min())
            Map_transaction_T_A_C_Y_Q = Transaction_of_Amount_Count_Year_Quarter(Map_transaction_T_A_C_Y, Quarters)
            
            column1, column2 = st.columns(2)
            with column1:
                States = st.selectbox('Select the State for bar chart view', Map_transaction_T_A_C_Y_Q['States'].unique())
            Map_Insurance_District(Map_transaction_T_A_C_Y_Q, States)
            
            
        elif method2 == 'Map User':
            column1, column2 = st.columns(2)
            with column1:
                years = st.slider('Select the year for Map User',MAP_USER['Years'].min(), MAP_USER['Years'].max(), MAP_USER['Years'].min())                 
            Map_User_Year = Map_User_Year_Wise_Plot(MAP_USER, years)
            
            column1, column2 = st.columns(2)
            with column1:
                Quarters = st.slider('Select the Quarter for Map User',Map_User_Year['Quarters'].min(), 
                                  Map_User_Year['Quarters'].max(), Map_User_Year['Quarters'].min())
            Map_User_Year_Quarter = Map_User_Quarter_Wise_Plot(Map_User_Year, Quarters)
            
            column1, column2 = st.columns(2)
            with column1:
                States = st.selectbox('Select the State for Map User', Map_User_Year_Quarter['States'].unique())
            Map_User_State_Wise_Plot(Map_User_Year_Quarter, States)
        
        
        
        
    with tab3 :
        method3 = st.radio('Select the method',['Top Insurance', 'Top Transaction', 'Top User'])
        
        if method3 == 'Top Insurance':
            
            column1, column2 = st.columns(2)
            with column1:
                years = st.slider('Select the year for Top Insurance',TOP_INSURANCE['Years'].min(), 
                                  TOP_INSURANCE['Years'].max(), TOP_INSURANCE['Years'].min())
            Top_Insurance_T_A_C_Y = Transaction_of_Amount_Count_Year(TOP_INSURANCE, years)
            
            column1, column2 = st.columns(2)
            with column1:
                States = st.selectbox('Select the State for Top Insurance', Top_Insurance_T_A_C_Y['States'].unique())
            Top_Insurance_Quarters_Pincode_Wise_Plot(Top_Insurance_T_A_C_Y, States)
            
            column1, column2 = st.columns(2)
            with column1:
                Quarters = st.slider('Select the Quarter for Top Insurance',Top_Insurance_T_A_C_Y['Quarters'].min(), 
                                  Top_Insurance_T_A_C_Y['Quarters'].max(), Top_Insurance_T_A_C_Y['Quarters'].min())
            Top_Insurance_T_A_C_Y_Q = Transaction_of_Amount_Count_Year_Quarter(Top_Insurance_T_A_C_Y, Quarters)
            
            
        elif method3 == 'Top Transaction':
            
            column1, column2 = st.columns(2)
            with column1:
                years = st.slider('Select the year for Top Transaction',TOP_TRANSACTION['Years'].min(), 
                                  TOP_TRANSACTION['Years'].max(), TOP_TRANSACTION['Years'].min())
            Top_Transaction_T_A_C_Y = Transaction_of_Amount_Count_Year(TOP_TRANSACTION, years)
            
            column1, column2 = st.columns(2)
            with column1:
                States = st.selectbox('Select the State for Top Transaction', Top_Transaction_T_A_C_Y['States'].unique())
            Top_Insurance_Quarters_Pincode_Wise_Plot(Top_Transaction_T_A_C_Y, States)
            
            column1, column2 = st.columns(2)
            with column1:
                Quarters = st.slider('Select the Quarter for Top Transaction',Top_Transaction_T_A_C_Y['Quarters'].min(), 
                                  Top_Transaction_T_A_C_Y['Quarters'].max(), Top_Transaction_T_A_C_Y['Quarters'].min())
            Top_Transaction_T_A_C_Y_Q = Transaction_of_Amount_Count_Year_Quarter(Top_Transaction_T_A_C_Y, Quarters)
            
            
        elif method3 == 'Top User':
            
            column1, column2 = st.columns(2)
            with column1:
                years = st.slider('Select the year for Top User',TOP_USER['Years'].min(), 
                                  TOP_USER['Years'].max(), TOP_USER['Years'].min())
            Top_User_T_A_C_Y = Top_User_Year_Quarters_Wise_Plot(TOP_USER, years)
            
            column1, column2 = st.columns(2)
            with column1:
                States = st.selectbox('Select the State for Top User', Top_User_T_A_C_Y['States'].unique())
            Top_User_T_A_C_Y_S = Top_User_States_Wise_Plot(Top_User_T_A_C_Y, States)

elif select == 'Top Charts':
    Questions = st.selectbox('Select the Question',['1. Transaction Amount and Count of Aggregated Insurance',
                                                    '2. Transaction Amount and Count of Map Insurance',
                                                    '3. Transaction Amount and Count of Top Insurance',
                                                    '4. Transaction Amount and Count of Aggregated Transaction',
                                                    '5. Transaction Amount and Count of Map Transaction',
                                                    '6. Transaction Amount and Count of Top Transaction',
                                                    '7. Transaction Count of Aggregated User',
                                                    '8. Registered User of Map User',
                                                    '9. App Opens of Map User',
                                                    '10. Registered User of Top User'])
    
    if Questions == '1. Transaction Amount and Count of Aggregated Insurance':
        st.subheader('TRANSACTION AMOUNT')
        Top_Chart_Transaction_Amount('aggregated_insurance')
        st.subheader('TRANSACTION COUNT')
        Top_Chart_Transaction_Count('aggregated_insurance')
        
    elif Questions == '2. Transaction Amount and Count of Map Insurance':
        st.subheader('TRANSACTION AMOUNT')
        Top_Chart_Transaction_Amount('map_insurance')
        st.subheader('TRANSACTION COUNT')
        Top_Chart_Transaction_Count('map_insurance')
        
    elif Questions == '3. Transaction Amount and Count of Top Insurance':
        st.subheader('TRANSACTION AMOUNT')
        Top_Chart_Transaction_Amount('top_insurance')
        st.subheader('TRANSACTION COUNT')
        Top_Chart_Transaction_Count('top_insurance')
        
    elif Questions == '4. Transaction Amount and Count of Aggregated Transaction':
        st.subheader('TRANSACTION AMOUNT')
        Top_Chart_Transaction_Amount('aggregated_transaction')
        st.subheader('TRANSACTION COUNT')
        Top_Chart_Transaction_Count('aggregated_transaction')
        
    elif Questions == '5. Transaction Amount and Count of Map Transaction':
        st.subheader('TRANSACTION AMOUNT')
        Top_Chart_Transaction_Amount('map_transaction')
        st.subheader('TRANSACTION COUNT')
        Top_Chart_Transaction_Count('map_transaction')
        
    elif Questions == '6. Transaction Amount and Count of Top Transaction':
        st.subheader('TRANSACTION AMOUNT')
        Top_Chart_Transaction_Amount('top_transaction')
        st.subheader('TRANSACTION COUNT')
        Top_Chart_Transaction_Count('top_transaction')
        
    elif Questions == '7. Transaction Count of Aggregated User':
        st.subheader('TRANSACTION COUNT')
        Top_Chart_Transaction_Count('aggregated_user')
        
    elif Questions == '8. Registered User of Map User':
        states = st.selectbox('Select the state', MAP_USER['States'].unique())
        st.subheader('REGISTERED USER')
        Top_Chart_Transaction_RegisteredUsers_count('map_user', states)
        
    elif Questions == '9. App Opens of Map User':
        states = st.selectbox('Select the state', MAP_USER['States'].unique())
        st.subheader('APP OPENS COUNT')
        Top_Chart_Transaction_AppOpens_count('map_user', states)
        
    elif Questions == '10. Registered User of Top User':
        st.subheader('REGISTERED USER')
        Top_Chart_RegisteredUsers_count('top_user')