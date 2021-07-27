
# DEPENDENCIES for R
# Necessary library dependencies for EQ/SQL/R

print("INSTALL DEPS EQ.SQL ...")

NCPUS = 16

r <- getOption("repos")
# r["CRAN"] <- "http://cran.cnr.berkeley.edu/"
r["CRAN"] <- "http://cran.wustl.edu/"
options(repos = r)

PKGS <- list(
    "R.utils"
    "devtools"
    "RPostgres"
)

for (pkg in PKGS) {
  print("")
  cat("INSTALL: ", pkg, "\n")
  # install.packages() does not return an error status
  install.packages(pkg, Ncpus=NCPUS, verbose=TRUE)
  print("")
  # Test that the pkg installed and is loadable
  cat("LOAD:    ", pkg, "\n")
  library(package=pkg, character.only=TRUE)
}

print("INSTALL DEPS EQ.SQL: OK")
