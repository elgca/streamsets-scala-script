package kw.common.data


import kw.common.Logging
import org.apache.commons.lang3.StringUtils
import org.apache.ivy._
import org.apache.ivy.core.module.descriptor._
import org.apache.ivy.core.module.id._
import org.apache.ivy.core.resolve._
import org.apache.ivy.core.settings._
import org.apache.ivy.plugins.resolver._


object EmbeddedIvy extends Logging {

  //  val repositories =
  //    ('repox, "http://repox.gtan.com:8078/") ::
  //      Nil

  case class TransitiveResolver(m2Compatible: Boolean, name: String, patternRoot: String) extends IBiblioResolver {
    setM2compatible(m2Compatible)
    setName(name)
    setRoot(patternRoot)
  }

  case class MavenCoordinate(groupId: String, artifactId: String, version: String) {
    override def toString: String = s"$groupId:$artifactId:$version"
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

  def resolveMavenDependencies(
                                packagesExclusions: String,
                                dependencies: String,
                                repositories: String
                              ): Seq[String] = {
    val exclusions: Seq[String] =
      if (!StringUtils.isBlank(packagesExclusions)) {
        packagesExclusions.split(",")
      } else {
        Nil
      }
    val repos: List[String] = if (!StringUtils.isBlank(repositories)) {
      repositories.split(",").toList
    } else {
      Nil
    }
    if (!StringUtils.isBlank(dependencies)) {
      val artifacts = extractMavenCoordinates(dependencies)
      artifacts.flatMap(x => resolve(repos,exclusions,x))
    } else {
      Nil
    }
  }

  /* Add any optional additional remote repositories */
  def processRemoteRepoArg(ivySettings: IvySettings, remoteRepos: List[String]): Unit = {
    val repositoryList = remoteRepos.toIterator.filter(x => x != null && !x.trim.isEmpty).map(_.trim).toList
    if (repositoryList.isEmpty) return
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
      log.info(s"$repo added as a remote repository with the name: ${brr.getName}")
    }

    ivySettings.addResolver(cr)
    ivySettings.setDefaultResolver(cr.getName)
  }

  def resolve(repositories: List[String], exclusions: Seq[String], artifacts: MavenCoordinate): Array[String] = {
    //creates clear ivy settings
    val ivySettings = new IvySettings()
    processRemoteRepoArg(ivySettings, repositories)
    //creates an Ivy instance with settings
    val ivy = Ivy.newInstance(ivySettings)
    val md =
      DefaultModuleDescriptor.newCallerInstance(
        ModuleRevisionId.newInstance(artifacts.groupId, artifacts.artifactId, artifacts.version),
        Array("*->default,!sources,!javadoc"), true, false
      )
    val ivyConfName = md.getDefaultConf
    exclusions.foreach { e =>
      md.addExcludeRule(createExclusion(e + ":*", ivySettings, ivyConfName))
    }
    //init resolve report
    val options = new ResolveOptions
    val report = ivy.resolve(md, options)
    //so you can get the jar library
    report.getAllArtifactsReports map (_.getLocalFile.toURI.toString)
  }

  //  def resolveScala(version: String): List[java.io.File] = {
  //    for {lib <- ScalaCoreLibraries.toList
  //         file <- resolve("org.scala-lang", lib, version)} yield file
  //  }

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
}