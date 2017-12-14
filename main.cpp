/**
*  @author  Rusty Riedel 2474883
*  @date    3/29/2017
*  @file    main.cpp
*  EECS 690 - Multicore & GPGPU Programming
*  Project02 - OpenMPI Census Data
*/

#include <fstream>
#include <sstream>
#include <iostream>
#include <string.h>
#include <mpi.h>
using namespace std;

//the dimensions of the csv file.
//NUMCITIES is the #of rows - 1
//NUMATTRIBUTES is the #cols (computed by colToIndex)
//FILENAME is the name of the input file
#define NUMCITIES 500
#define NUMATTRIBUTES colToIndex("DM")
#define FILENAME "500_Cities__City-level_Data__GIS_Friendly_Format_.csv"

//converts the csv column header "AA" etc.. to an N-1 index
int colToIndex(string s)
{
    int ret = 0;
    for (int i = 0; i < s.length(); i ++) {
        ret = ret * 26 + s[i] - 64;
    }
    return ret - 1;
}

//parses an input file into its respective elements
void parseFile(string file, string headers[], string places[][NUMCITIES],
    double globaldata[][NUMCITIES])
{
    //open the csv file to parse
    ifstream dataFile(file);
    string temp;

    //get the column headers 
    getline(dataFile, temp);
    stringstream temp2(temp);
    string t;
    int i = 0;
    for (int i = 0; i < NUMATTRIBUTES + 1; i++)
    {
        getline(temp2, t, ',');
        headers[i] = t;
    }

    //parse the data for each city
    int row = 0;
    int col = 0;
    while (getline(dataFile, temp))
    {
        stringstream temp2(temp);
        string data;
        while (getline(temp2, data, '\"'))
        {
            if (data[0] != '(')
            {
                stringstream temp3(data);
                string data2;
                while (getline(temp3, data2, ','))
                {
                    if (data2 != "")
                    {   
                        //store the state and city in the places array
                        if(col < 2)
                            places[col][row] = data2;
                        //all other data goes in the globaldata array
                        else
                            globaldata[col - 2][row] = stod(data2);
                        col++;
                    }
                }
            }
            else
            {   
                //range data default to -1 because we wont use them
                globaldata[col - 2][row] = -1;
                col++;
            }
        }
        row++;
        col = 0;
    }//end parsing file

    dataFile.close();
}

//gets the smallest number from the data array
double getMin(double data[], int range)
{
    double min = data[0];
    for(int i = 0; i < range; i++)
        if(data[i] < min)
            min = data[i];

    return min;
}

//gets the largest number from the data array
double getMax(double data[], int range)
{
    double max = data[0];
    for(int i = 0; i < range; i++)
        if(data[i] > max)
            max = data[i];

    return max;
}

//calculates the average from the data array
double getSum(double data[], int range)
{
    double sum = 0;
    for(int i = 0; i < range; i++)
        sum += data[i];

    return sum;
}

//returns the number of cities that are greater than or less than
//the given value in the given data set.
int getNum(double data[], int range, char* cond, double val)
{   
    int total = 0;
    //calculate number of cities in datat that are greater than val
    if(!strcmp(cond, "gt"))
    {
        for(int i = 0; i < range; i++)
            if (data[i] > val)
                total++;
    }
    //calalculate number of cities in data that are less than val 
    else
    {   
        for(int i = 0; i < range; i++)
            if (data[i] < val)
                total++;
    }
    return total;
}

//returns a string of the location that corresponds to the given value
string placeLookup(double data[], double val, string places[][NUMCITIES])
{
    for(int i = 0; i < NUMCITIES; i++)
        if(data[i] == val)
            return (places[1][i] + ", " + places[0][i] + ", ");
}

int main (int argc, char *argv[])
{   
    int size, rank;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    //arrays to hold parsed information
    string headers[NUMATTRIBUTES + 1];
    string places[2][NUMCITIES];
    double globaldata[NUMATTRIBUTES - 1][NUMCITIES];

    //determine if the mode should be sr or bg
    if(!strcmp(argv[1], "sr"))
    {   
        double result;
        double localdata[(NUMCITIES/size)];
        string output = "";

        //check if NUMCITIES is divisible by the number of processes (-np #)
        if((NUMCITIES % size) != 0)
        {
            //not evenly divisable so exit the program with an error
            if(rank == 0)
            {
                cerr << "The number of processes does not evenly divide " << NUMCITIES << endl;
                cerr << "Program terminating!\n";
            }
            MPI_Finalize();
            return 0;
        }

        if (rank == 0) {
            //parse the input csv file
            parseFile(FILENAME, headers, places, globaldata);

            //scatters the data
            MPI_Scatter(globaldata[colToIndex(argv[3]) - 2], (NUMCITIES/size), 
                MPI_DOUBLE, localdata, (NUMCITIES/size), MPI_DOUBLE, 0, MPI_COMM_WORLD);
        }
        else
            //each process gets its partition of the column as localdata
            MPI_Scatter(nullptr, (NUMCITIES/size), MPI_DOUBLE, localdata, 
                (NUMCITIES/size), MPI_DOUBLE, 0, MPI_COMM_WORLD);

        //determine what operation needs to be done (min/max/avg/number)
        if(!strcmp(argv[2], "min"))
        {
            double min = getMin(localdata, (NUMCITIES/size));
            MPI_Reduce(&min, &result, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
            
            //make the output string
            if(rank == 0)
            {
                string pl = placeLookup(globaldata[colToIndex(argv[3]) - 2], result, places);
                output += pl + headers[colToIndex(argv[3])] + " = ";
            }
        }
        else if(!strcmp(argv[2], "max"))
        {
            double max = getMax(localdata, (NUMCITIES/size));
            MPI_Reduce(&max, &result, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
            
            //make the output string
            if(rank == 0)
            {
                string pl = placeLookup(globaldata[colToIndex(argv[3]) - 2], result, places);
                output += pl + headers[colToIndex(argv[3])] + " = ";
            }
        }
        else if(!strcmp(argv[2], "avg"))
        {
            double avg = getSum(localdata, (NUMCITIES/size));
            MPI_Reduce(&avg, &result, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
            
            //make the output string
            if(rank == 0)
            {
                result /= NUMCITIES;
                output += "Average " + headers[colToIndex(argv[3])] + " = ";
            }
        }
        else if(!strcmp(argv[2], "number"))
        {
            double num = getNum(localdata, (NUMCITIES/size), argv[4], stod(argv[5]));
            MPI_Reduce(&num, &result, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
            
            //make the output string
            if(rank == 0)
            {
                string val(argv[5]);
                string comp(argv[4]);
                output += "Number cities with " + headers[colToIndex(argv[3])] + " ";
                output += comp + " " + val + " = ";
            }
        }
        
        //output the result
        if (rank == 0)
            cout << output << result << endl;
    }//end scatter-reduce
    //Broadcast - Gather (point-to-point for effeciency)
    else
    {
        double results[size];
        double result;
        double localdata[NUMCITIES];
        int numColsToProc = argc - 3;
        MPI_Status status;

        //check if the number of process is equal to the number of columns to scan
        if(numColsToProc != size)
        {
            //number of processes != number of columns, so exit the program with an error
            if(rank == 0)
            {
                cerr << "The number of processes is not the same as number of columns to scan!" << endl;
                cerr << "Program terminating!\n";
            }
            MPI_Finalize();
            return 0;
        }

        if (rank == 0) {
            //initialize the result set to -1s
            for (int i = 0; i < size; i++)
                results[i] = -1;

            //parse the input csv file
            parseFile(FILENAME, headers, places, globaldata);

            //send a column of data to each process
            for (int receiver = 0; receiver < size; receiver++)
                MPI_Send(globaldata[colToIndex(argv[3 + receiver]) - 2], 
                    NUMCITIES, MPI_DOUBLE, receiver, 1, MPI_COMM_WORLD);
        }

        //let each rank recieve the data
        MPI_Recv(localdata, NUMCITIES, MPI_DOUBLE, 0, 1, MPI_COMM_WORLD, &status);

        //determine what operation needs to be done (min/max/avg/number)
        if(!strcmp(argv[2], "min"))
        {
            double min = getMin(localdata, NUMCITIES);
            MPI_Gather(&min, 1, MPI_DOUBLE, results, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
        }
        else if(!strcmp(argv[2], "max"))
        {
            double max = getMax(localdata, NUMCITIES);
            MPI_Gather(&max, 1, MPI_DOUBLE, results, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
        }
        else if(!strcmp(argv[2], "avg"))
        {
            double avg = getSum(localdata, NUMCITIES);
            avg /= NUMCITIES;
            MPI_Gather(&avg, 1, MPI_DOUBLE, results, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
        }

        //display the results of the computations
        if(rank == 0)
            for(int i = 0; i < size; i++)
                cout << argv[2] << " " << headers[colToIndex(argv[3 + i])] << " = " << results[i] << endl;
    }//end broadcast-gather

    //exit the program (successful termination)
    MPI_Finalize();
    return 0;
}