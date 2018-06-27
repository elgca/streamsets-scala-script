package kw.common.data

import java.io.{File, _}
import java.net.{URI, URISyntaxException}
import java.nio.charset.Charset
import java.text.ParseException
import java.util.UUID

import kw.common.{LogWriter, Logging}
import org.apache.commons.io.Charsets
import org.apache.commons.io.output.WriterOutputStream
import org.apache.commons.lang3.StringUtils
import org.apache.ivy.Ivy
import org.apache.ivy.core.LogOptions
import org.apache.ivy.core.module.descriptor._
import org.apache.ivy.core.module.id.{ArtifactId, ModuleId, ModuleRevisionId}
import org.apache.ivy.core.report.ResolveReport
import org.apache.ivy.core.resolve.ResolveOptions
import org.apache.ivy.core.retrieve.RetrieveOptions
import org.apache.ivy.core.settings.IvySettings
import org.apache.ivy.plugins.matcher.GlobPatternMatcher
import org.apache.ivy.plugins.repository.file.FileRepository
import org.apache.ivy.plugins.resolver.{ChainResolver, FileSystemResolver, IBiblioResolver}

/**
  * copy from sparksubmitutil
  */
object DependencyUtils extends Logging {
  val printStream: PrintStream = new PrintStream(new WriterOutputStream(new LogWriter(log), "utf8"))

  /**
    * Return a well-formed URI for the file described by a user input string.
    *
    * If the supplied path does not contain a scheme, or is a relative path, it will be
    * converted into an absolute path with a file:// scheme.
    */
  def resolveURI(path: String): URI = {
    try {
      val uri = new URI(path)
      if (uri.getScheme() != null) {
        return uri
      }
      // make sure to handle if the path has a fragment (applies to yarn
      // distributed cache)
      if (uri.getFragment() != null) {
        val absoluteURI = new File(uri.getPath()).getAbsoluteFile().toURI()
        return new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
          uri.getFragment())
      }
    } catch {
      case e: URISyntaxException =>
    }
    new File(path).getAbsoluteFile().toURI()
  }

  // Exposed for testing.
  // These components are used to make the default exclusion rules for Spark dependencies.
  // We need to specify each component explicitly, otherwise we miss spark-streaming-kafka-0-8 and
  // other spark-streaming utility components. Underscore is there to differentiate between
  // spark-streaming_2.1x and spark-streaming-kafka-0-8-assembly_2.1x
  val IVY_DEFAULT_EXCLUDES = Seq("catalyst_", "core_", "graphx_", "kvstore_", "launcher_", "mllib_",
    "mllib-local_", "network-common_", "network-shuffle_", "repl_", "sketch_", "sql_", "streaming_",
    "tags_", "unsafe_")

  case class MavenCoordinate(groupId: String, artifactId: String, version: String) {
    override def toString: String = s"$groupId:$artifactId:$version"
  }

  def buildIvySettings(remoteRepos: Seq[String], ivyPath: Option[String]): IvySettings = {
    val ivySettings: IvySettings = new IvySettings
    processIvyPathArg(ivySettings, ivyPath)

    // create a pattern matcher
    ivySettings.addMatcher(new GlobPatternMatcher)
    // create the dependency resolvers
    val repoResolver = createRepoResolvers(ivySettings.getDefaultIvyUserDir)
    ivySettings.addResolver(repoResolver)
    ivySettings.setDefaultResolver(repoResolver.getName)
    //    val repositoryList = remoteRepos.filterNot(_.trim.isEmpty).map(_.split(",").toList).getOrElse(Nil)
    processRemoteRepoArg(ivySettings, remoteRepos)
    ivySettings
  }

  /** Path of the local Maven cache. */
  def m2Path: File = new File(System.getProperty("user.home"), ".m2" + File.separator + "repository")

  /**
    * Extracts maven coordinates from a comma-delimited string
    *
    * @param defaultIvyUserDir The default user path for Ivy
    * @return A ChainResolver used by Ivy to search for and resolve dependencies.
    */
  def createRepoResolvers(defaultIvyUserDir: File): ChainResolver = {
    // We need a chain resolver if we want to check multiple repositories
    val cr = new ChainResolver
    cr.setName("spark-list")

    val localM2 = new IBiblioResolver
    localM2.setM2compatible(true)
    localM2.setRoot(m2Path.toURI.toString)
    localM2.setUsepoms(true)
    localM2.setName("local-m2-cache")
    cr.add(localM2)

    val localIvy = new FileSystemResolver
    val localIvyRoot = new File(defaultIvyUserDir, "local")
    localIvy.setLocal(true)
    localIvy.setRepository(new FileRepository(localIvyRoot))
    val ivyPattern = Seq(localIvyRoot.getAbsolutePath, "[organisation]", "[module]", "[revision]",
      "ivys", "ivy.xml").mkString(File.separator)
    localIvy.addIvyPattern(ivyPattern)
    val artifactPattern = Seq(localIvyRoot.getAbsolutePath, "[organisation]", "[module]",
      "[revision]", "[type]s", "[artifact](-[classifier]).[ext]").mkString(File.separator)
    localIvy.addArtifactPattern(artifactPattern)
    localIvy.setName("local-ivy-cache")
    cr.add(localIvy)

    // the biblio resolver resolves POM declared dependencies
    val br: IBiblioResolver = new IBiblioResolver
    br.setM2compatible(true)
    br.setUsepoms(true)
    br.setName("central")
    cr.add(br)

    val sp: IBiblioResolver = new IBiblioResolver
    sp.setM2compatible(true)
    sp.setUsepoms(true)
    sp.setRoot("http://dl.bintray.com/spark-packages/maven")
    sp.setName("spark-packages")
    cr.add(sp)
    cr
  }

  /**
    * Load Ivy settings from a given filename, using supplied resolvers
    *
    * @param settingsFile Path to Ivy settings file
    * @param remoteRepos  Comma-delimited string of remote repositories other than maven central
    * @param ivyPath      The path to the local ivy repository
    * @return An IvySettings object
    */
  def loadIvySettings(
                       settingsFile: String,
                       remoteRepos: Seq[String],
                       ivyPath: Option[String]): IvySettings = {
    val file = new File(settingsFile)
    require(file.exists(), s"Ivy settings file $file does not exist")
    require(file.isFile(), s"Ivy settings file $file is not a normal file")
    val ivySettings: IvySettings = new IvySettings
    try {
      ivySettings.load(file)
    } catch {
      case e@(_: IOException | _: ParseException) =>
        throw new Exception(s"Failed when loading Ivy settings from $settingsFile", e)
    }
    processIvyPathArg(ivySettings, ivyPath)
    processRemoteRepoArg(ivySettings, remoteRepos)
    ivySettings
  }

  /* Add any optional additional remote repositories */
  def processRemoteRepoArg(ivySettings: IvySettings, repositoryList: Seq[String]): Unit = {
    if (repositoryList.nonEmpty) {
      val cr = new ChainResolver
      cr.setName("user-list")

      // add current default resolver, if any
      Option(ivySettings.getDefaultResolver).foreach(cr.add)

      // add additional repositories, last resolution in chain takes precedence
      repositoryList.zipWithIndex.foreach { case (repo, i) =>
        val brr: IBiblioResolver = new IBiblioResolver
        brr.setM2compatible(true)
        brr.setUsepoms(true)
        brr.setRoot(repo)
        brr.setName(s"repo-${i + 1}")
        cr.add(brr)
        // scalastyle:off println
        printStream.println(s"$repo added as a remote repository with the name: ${brr.getName}")
        // scalastyle:on println
      }

      ivySettings.addResolver(cr)
      ivySettings.setDefaultResolver(cr.getName)
    }
  }

  /* Set ivy settings for location of cache, if option is supplied */
  def processIvyPathArg(ivySettings: IvySettings, ivyPath: Option[String]): Unit = {
    ivyPath.filterNot(_.trim.isEmpty).foreach { alternateIvyDir =>
      ivySettings.setDefaultIvyUserDir(new File(alternateIvyDir))
      ivySettings.setDefaultCache(new File(alternateIvyDir, "cache"))
    }
  }

  /**
    * Resolves any dependencies that were supplied through maven coordinates
    *
    * @param artifacts   Comma-delimited string of maven coordinates
    * @param ivySettings An IvySettings containing resolvers to use
    * @param exclusions  Exclusions to apply when resolving transitive dependencies
    * @return The comma-delimited path to the jars of the given maven artifacts including their
    *         transitive dependencies
    */
  def resolveMavenCoordinates(
                               artifacts: Seq[MavenCoordinate],
                               ivySettings: IvySettings,
                               exclusions: Seq[String] = Nil,
                               isTest: Boolean = false): Array[String] = {
    if (artifacts == null || artifacts.isEmpty) {
      Array.empty[String]
    } else {
      val sysOut = System.out
      try {
        // To prevent ivy from logging to system out
        System.setOut(printStream)
        //        val artifacts = extractMavenCoordinates(coordinates)
        // Directories for caching downloads through ivy and storing the jars when maven coordinates
        // are supplied to spark-submit
        val packagesDirectory: File = new File(ivySettings.getDefaultIvyUserDir, "jars")
        // scalastyle:off println
        printStream.println(
          s"Ivy Default Cache set to: ${ivySettings.getDefaultCache.getAbsolutePath}")
        printStream.println(s"The jars for the packages stored in: $packagesDirectory")
        // scalastyle:on println

        val ivy = Ivy.newInstance(ivySettings)
        // Set resolve options to download transitive dependencies as well
        val resolveOptions = new ResolveOptions
        resolveOptions.setTransitive(true)
        val retrieveOptions = new RetrieveOptions
        resolveOptions.setDownload(true)

        // Default configuration name for ivy
        val ivyConfName = "default"

        // A Module descriptor must be specified. Entries are dummy strings
        val md = getModuleDescriptor

        md.setDefaultConf(ivyConfName)

        // Add exclusion rules for Spark and Scala Library
        addExclusionRules(ivySettings, ivyConfName, md)
        // add all supplied maven artifacts as dependencies
        addDependenciesToIvy(md, artifacts, ivyConfName)
        exclusions.foreach { e =>
          md.addExcludeRule(createExclusion(e + ":*", ivySettings, ivyConfName))
        }
        // resolve dependencies
        val rr: ResolveReport = ivy.resolve(md, resolveOptions)
        if (rr.hasError) {
          throw new RuntimeException(rr.getAllProblemMessages.toString)
        }
        // retrieve all resolved dependencies
        ivy.retrieve(rr.getModuleDescriptor.getModuleRevisionId,
          packagesDirectory.getAbsolutePath + File.separator +
            "[organization]_[artifact]-[revision](-[classifier]).[ext]",
          retrieveOptions.setConfs(Array(ivyConfName)))
        val paths = resolveDependencyPaths(rr.getArtifacts.toArray, packagesDirectory)
        val mdId = md.getModuleRevisionId
        clearIvyResolutionFiles(mdId, ivySettings, ivyConfName)
        paths
      } finally {
        System.setOut(sysOut)
      }
    }
  }

  def extractMavenCoordinates(coordinates: String): Seq[MavenCoordinate] = {
    coordinates.split(",").filter(_.trim.nonEmpty).map { p =>
      val splits = p.replace("/", ":").split(":")
      require(splits.length == 3, s"Provided Maven Coordinates must be in the form " +
        s"'groupId:artifactId:version'. The coordinate provided is: $p")
      require(splits(0) != null && splits(0).trim.nonEmpty, s"The groupId cannot be null or " +
        s"be whitespace. The groupId provided is: ${splits(0)}")
      require(splits(1) != null && splits(1).trim.nonEmpty, s"The artifactId cannot be null or " +
        s"be whitespace. The artifactId provided is: ${splits(1)}")
      require(splits(2) != null && splits(2).trim.nonEmpty, s"The version cannot be null or " +
        s"be whitespace. The version provided is: ${splits(2)}")
      new MavenCoordinate(splits(0), splits(1), splits(2))
    }
  }

  def getModuleDescriptor: DefaultModuleDescriptor = DefaultModuleDescriptor.newDefaultInstance(
    // Include UUID in module name, so multiple clients resolving maven coordinate at the same time
    // do not modify the same resolution file concurrently.
    ModuleRevisionId.newInstance("org.apache.spark",
      s"spark-submit-parent-${UUID.randomUUID.toString}",
      "1.0"))

  /** Add exclusion rules for dependencies already included in the spark-assembly */
  def addExclusionRules(
                         ivySettings: IvySettings,
                         ivyConfName: String,
                         md: DefaultModuleDescriptor): Unit = {
    // Add scala exclusion rule
    md.addExcludeRule(createExclusion("*:scala-library:*", ivySettings, ivyConfName))

//    IVY_DEFAULT_EXCLUDES.foreach { comp =>
//      md.addExcludeRule(createExclusion(s"org.apache.spark:spark-$comp*:*", ivySettings,
//        ivyConfName))
//    }
  }

  /** Adds the given maven coordinates to Ivy's module descriptor. */
  def addDependenciesToIvy(
                            md: DefaultModuleDescriptor,
                            artifacts: Seq[MavenCoordinate],
                            ivyConfName: String): Unit = {
    artifacts.foreach { mvn =>
      val ri = ModuleRevisionId.newInstance(mvn.groupId, mvn.artifactId, mvn.version)
      val dd = new DefaultDependencyDescriptor(ri, false, false)
      dd.addDependencyConfiguration(ivyConfName, ivyConfName + "(runtime)")
      // scalastyle:off println
      printStream.println(s"${dd.getDependencyId} added as a dependency")
      // scalastyle:on println
      md.addDependency(dd)
    }
  }

  def createExclusion(
                       coords: String,
                       ivySettings: IvySettings,
                       ivyConfName: String): ExcludeRule = {
    val c = extractMavenCoordinates(coords)(0)
    val id = new ArtifactId(new ModuleId(c.groupId, c.artifactId), "*", "*", "*")
    val rule = new DefaultExcludeRule(id, ivySettings.getMatcher("glob"), null)
    rule.addConfiguration(ivyConfName)
    rule
  }

  /**
    * Output a comma-delimited list of paths for the downloaded jars to be added to the classpath
    * (will append to jars in SparkSubmit).
    *
    * @param artifacts      Sequence of dependencies that were resolved and retrieved
    * @param cacheDirectory directory where jars are cached
    * @return a comma-delimited list of paths for the dependencies
    */
  def resolveDependencyPaths(
                              artifacts: Array[AnyRef],
                              cacheDirectory: File) = {
    artifacts.map { artifactInfo =>
      val artifact = artifactInfo.asInstanceOf[Artifact].getModuleRevisionId
      cacheDirectory.getAbsolutePath + File.separator +
        s"${artifact.getOrganisation}_${artifact.getName}-${artifact.getRevision}.jar"
    }
  }

  /**
    * Clear ivy resolution from current launch. The resolution file is usually at
    * ~/.ivy2/org.apache.spark-spark-submit-parent-$UUID-default.xml,
    * ~/.ivy2/resolved-org.apache.spark-spark-submit-parent-$UUID-1.0.xml, and
    * ~/.ivy2/resolved-org.apache.spark-spark-submit-parent-$UUID-1.0.properties.
    * Since each launch will have its own resolution files created, delete them after
    * each resolution to prevent accumulation of these files in the ivy cache dir.
    */
  private def clearIvyResolutionFiles(
                                       mdId: ModuleRevisionId,
                                       ivySettings: IvySettings,
                                       ivyConfName: String): Unit = {
    val currentResolutionFiles = Seq(
      s"${mdId.getOrganisation}-${mdId.getName}-$ivyConfName.xml",
      s"resolved-${mdId.getOrganisation}-${mdId.getName}-${mdId.getRevision}.xml",
      s"resolved-${mdId.getOrganisation}-${mdId.getName}-${mdId.getRevision}.properties"
    )
    currentResolutionFiles.foreach { filename =>
      new File(ivySettings.getDefaultCache, filename).delete()
    }
  }


  def resolveMavenDependencies(
                                exclusions: Seq[String],
                                artifacts: Seq[MavenCoordinate],
                                repositories: Seq[String],
                                ivyRepoPath: String,
                                ivySettingsPath: Option[String]) = {
    // Create the IvySettings, either load from file or build defaults
    val ivySettings = ivySettingsPath match {
      case Some(path) =>
        loadIvySettings(path, repositories, Option(ivyRepoPath))

      case None =>
        buildIvySettings(repositories, Option(ivyRepoPath))
    }
    resolveMavenCoordinates(artifacts, ivySettings, exclusions = exclusions)
  }

  def addJarToClasspath(localJar: String, loader: MutableURLClassLoader): Unit = {
    val uri = resolveURI(localJar)
    uri.getScheme match {
      case "file" | "local" =>
        val file = new File(uri.getPath)
        if (file.exists()) {
          loader.addURL(file.toURI.toURL)
        } else {
          log.warn(s"Local jar $file does not exist, skipping.")
        }
      case _ =>
        log.warn(s"Skip remote jar $uri.")
    }
  }

}