''' NDMP provides for a server extension mechanism that enables the 
               following: 

                  - The NDMP community can develop and standardize new 
                  functionality in NDMP without requiring a revision of core NDMP 

                  - Implementers can expose proprietary functionality in NDMP 
                  Server implementations through NDMP Server extensions 

                  - DMAs can discover and negotiate the use of these extensions 

                  - Extensions are managed at two levels: standard extensions 
                  developed or ratified by the NDMP community, and proprietary 
                  extensions developed for the individual implementations 

                  - Extensions are versioned, and can evolve over time 
'''

from ctypes import Structure


class ndmp_test_echo_request(Structure):
    _fields_ = [()]


class ndmp_test_echo_reply(Structure):
    _fields_ = [()]


    
    
    