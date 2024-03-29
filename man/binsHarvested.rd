% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/binsHarvested.R
\name{binsHarvested}
\alias{binsHarvested}
\title{bin harvested summary}
\usage{
binsHarvested(seasons, password)
}
\arguments{
\item{seasons}{a vector of the seasons to be returned.}

\item{password}{a text string containing the password to the SQL database (see administrator for the password string)}
}
\value{
a tibble with each row representing a specific bin consignment.
}
\description{
`binsHarvested` returns a tibble which contains the summary information on bins harvested
}
\details{
The `binsHarvested` function is a helper function to return a useful tibble to allow more detailed analysis
of bins submitted into the ABC system (at the gatehouse). The function returns all recorded bin consignments by season
and Bin delivery number.
}
\seealso{
[graderFunction()], [defectAssessment()], [growerRTE()]
}
