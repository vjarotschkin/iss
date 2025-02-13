
**Update 2025-02-13**

Your compiler **is** being passed -std=gnu++17, but **systemfonts** also contains Objective-C++ files (.mm), which can require a separate flag. Specifically, .mm files look at **OBJCXXFLAGS** rather than **CXX17FLAGS**. Even though you see -std=gnu++17 on the command line, the Objective-C++ compiler might not be honoring it fully in this environment. That explains the complaints about constexpr and other C++11/C++17 features being unrecognized.

  

Below is the most common fix on macOS for projects with Objective-C++ code:

**1. Update ~/.R/Makevars to include OBJCXXFLAGS**

  

Open or create ~/.R/Makevars and add **both** CXX17FLAGS (already there) **and** OBJCXXFLAGS, for example:

```
CXX17FLAGS=-std=gnu++17 -stdlib=libc++ -g -O2
OBJCXXFLAGS=-std=gnu++17 -stdlib=libc++ -g -O2
```

Save the file, then **restart R / RStudio** so these flags take effect in every session, including renv projects.

**2. Rebuild the Key Packages**

  

Within your renv-activated project, rebuild the packages that failed to compile:

```
renv::activate()
renv::install("cpp11", rebuild = TRUE)
renv::install("systemfonts", rebuild = TRUE)
renv::install("tidyverse")
```

This ensures **cpp11** and **systemfonts** are compiled using the new flags.

**3. Verify the SDK (Optional)**

  

If errors persist, check that your command-line tools are up-to-date and that R knows which SDK to use:

1. **Install / Update Xcode CLT**

```
xcode-select --install
```

  

2. **Confirm the SDK** is found. In your R session:

```
Sys.getenv("SDKROOT")
```

If it’s empty, try setting it to the command-line tools SDK path (e.g. /Library/Developer/CommandLineTools/SDKs/MacOSX13.0.sdk) before reinstalling:

```
Sys.setenv(SDKROOT = "/Library/Developer/CommandLineTools/SDKs/MacOSX13.0.sdk")
renv::install("systemfonts", rebuild = TRUE)
```

**Why This Helps:**

• **Objective-C++ files (.mm)** do not automatically use the same flags as standard C++ files unless OBJCXXFLAGS is also set.

• systemfonts on macOS includes Objective-C++ code in mac/FontManagerMac.mm, so if the correct standard (C++17) isn’t applied there, you’ll see errors about constexpr, rvalue references, and std::tuple.

  

Once OBJCXXFLAGS is properly set, you should be able to compile **systemfonts** (and therefore **tidyverse**) inside your renv environment.


--> this worked!