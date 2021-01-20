import os
import re
import sys
import subprocess
import zipfile as zf

def unpackFile( scene, exp, out_path ):

    """              
    unpack single file from zip file
    """

    # unpack single file - otherwise throw
    out_files = unpackFiles( scene, exp, out_path )

    if len ( out_files ) != 1:
        RaiseRuntimeException ( 'Expected single file matching expression {} in dataset: {} - found {} files'.format ( exp, scene, len( out_files ) ) )

    return out_files[ 0 ]    


def unpackFiles( scene, exp, out_path ):

    """              
    unpack selected files from zip file
    """
    
    # open archive and iterate on contents
    out_files = []
    with zf.ZipFile( scene ) as z:

        files = z.namelist()
        for f in files:

            # apply regexp on filenames
            m = re.match ( exp, f )
            if m:

                # extract file if name matches
                z.extract( f, out_path )
                out_files.append( os.path.join( out_path, f ) )

    return out_files


def matchFile( files, exp ):

    """              
    match list of pathname strings with regular expression
    """
    
    # match single file or throw
    out = matchFiles( files, exp )
    if len ( out ) != 1:
        RaiseRuntimeException ( 'Expected single file matching expression {} in dataset: {} - found {} files'.format ( exp, scene, len( out_files ) ) )

    return out[ 0 ]


def matchFiles( files, exp ):

    """              
    match list of pathname strings with regular expression
    """
    
    # for each entry in argument
    out = []
    for f in files:

        # apply regexp on pathname
        m = re.match ( exp, f )
        if m:

            # update match list
            out.append ( f )

    return out


def findItems( obj, field ):

    """              
    recursively extract key values from dictionary
    """

    # for all key value pairs
    values = []
    for key, value in obj.items():

        # record value of key match
        if key == field:
            values.append( value )

        # recursive call on nested dict
        elif isinstance( value, dict ):
            results = findItems( value, field )
            for result in results:
                values.append( result )

        # loop through contents in array
        elif isinstance( value, list ):
            for item in value:

                # recursive call on nested dict
                if isinstance( item, dict ):
                    results = findItems( item, field )
                    for result in results:
                        values.append( result )

    return values        


def execute( name, arguments ):

    """              
    create and execute sub-process
    """

    # create sub-process with argument list
    p = subprocess.Popen( [name] + arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    out, err = p.communicate()
    code = p.poll();

    return out, err, code


