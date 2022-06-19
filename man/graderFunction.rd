\name{graderFunction}
\alias{graderFunction}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Rockit helper function for grader batch data
}
\description{
A helper function for Rockit R users  - takes a vector of years as an input, makes a call directly to the ABC SQL database and outputs a table of consignments and related proporties.
}
\usage{
graderFunction(x)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{
a vector of years included in the data (default is all years)
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\references{
%% ~put references to the literature/web site here ~
}
\author{
Stuart Dykes
}
\note{
the package require ODBC driver is installed
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.

## must define the years you want as a vector
graderBatchData <- graderFunction(c(2016, 2017, 2018))
}

% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory (show via RShowDoc("KEYWORDS")):
% \keyword{ ~kwd1 }
% \keyword{ ~kwd2 }
% Use only one keyword per line.
% For non-standard keywords, use \concept instead of \keyword:
% \concept{ ~cpt1 }
% \concept{ ~cpt2 }
% Use only one concept per line.
