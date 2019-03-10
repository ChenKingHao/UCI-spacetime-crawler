'''
Created on Oct 20, 2016
@author: Rohan Achar
'''
from rtypes.pcc.attributes import dimension, primarykey, predicate
from rtypes.pcc.types.subset import subset
from rtypes.pcc.types.set import pcc_set
from rtypes.pcc.types.projection import projection
from rtypes.pcc.types.impure import impure
from datamodel.search.server_datamodel import Link, ServerCopy

@pcc_set
class Haoc19Mutianx1Link(Link):
    USERAGENTSTRING = "Haoc19Mutianx1"

    @dimension(str)
    def user_agent_string(self):
        return self.USERAGENTSTRING

    @user_agent_string.setter
    def user_agent_string(self, v):
        # TODO (rachar): Make it such that some dimensions do not need setters.
        pass


@subset(Haoc19Mutianx1Link)
class Haoc19Mutianx1UnprocessedLink(object):
    @predicate(Haoc19Mutianx1Link.download_complete, Haoc19Mutianx1Link.error_reason)
    def __predicate__(download_complete, error_reason):
        return not (download_complete or error_reason)


@impure
@subset(Haoc19Mutianx1UnprocessedLink)
class OneHaoc19Mutianx1UnProcessedLink(Haoc19Mutianx1Link):
    __limit__ = 1

    @predicate(Haoc19Mutianx1Link.download_complete, Haoc19Mutianx1Link.error_reason)
    def __predicate__(download_complete, error_reason):
        return not (download_complete or error_reason)

@projection(Haoc19Mutianx1Link, Haoc19Mutianx1Link.url, Haoc19Mutianx1Link.download_complete)
class Haoc19Mutianx1ProjectionLink(object):
    pass
