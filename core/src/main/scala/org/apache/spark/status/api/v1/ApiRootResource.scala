/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.status.api.v1

import java.util.zip.ZipOutputStream
import javax.servlet.ServletContext

import org.eclipse.jetty.server.handler.ContextHandler
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}

import org.apache.spark.SecurityManager
import org.apache.spark.ui.SparkUI

/**
 * Main entry point for serving spark application metrics as json, using JAX-RS.
 *
 * Each resource should have endpoints that return **public** classes defined in api.scala.  Mima
 * binary compatibility checks ensure that we don't inadvertently make changes that break the api.
 * The returned objects are automatically converted to json by jackson with JacksonMessageWriter.
 * In addition, there are a number of tests in HistoryServerSuite that compare the json to "golden
 * files".  Any changes and additions should be reflected there as well -- see the notes in
 * HistoryServerSuite.
 */
private[v1] class ApiRootResource extends UIRootFromServletContext {
  
  def getApplicationList(): ApplicationListResource = {
    new ApplicationListResource(uiRoot)
  }
  
  def getApplication(): OneApplicationResource = {
    new OneApplicationResource(uiRoot)
  }
  
  def getJobs(
      appId: String,
      attemptId: String): AllJobsResource = {
    uiRoot.withSparkUI(appId, Some(attemptId)) { ui =>
      new AllJobsResource(ui)
    }
  }
  
  def getJobs( appId: String): AllJobsResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new AllJobsResource(ui)
    }
  }
  
  def getJob( appId: String): OneJobResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new OneJobResource(ui)
    }
  }
  
  def getJob(
      appId: String,
      attemptId: String): OneJobResource = {
    uiRoot.withSparkUI(appId, Some(attemptId)) { ui =>
      new OneJobResource(ui)
    }
  }
  
  def getExecutors( appId: String): ExecutorListResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new ExecutorListResource(ui)
    }
  }
  
  def getExecutors(
  appId: String,
     attemptId: String): ExecutorListResource = {
    uiRoot.withSparkUI(appId, Some(attemptId)) { ui =>
      new ExecutorListResource(ui)
    }
  }

  
  def getStages(appId: String): AllStagesResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new AllStagesResource(ui)
    }
  }
  
  def getStages(
      appId: String,
      attemptId: String): AllStagesResource = {
    uiRoot.withSparkUI(appId, Some(attemptId)) { ui =>
      new AllStagesResource(ui)
    }
  }

 
  def getStage( appId: String): OneStageResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new OneStageResource(ui)
    }
  }
  
  def getStage(
      appId: String,
      attemptId: String): OneStageResource = {
    uiRoot.withSparkUI(appId, Some(attemptId)) { ui =>
      new OneStageResource(ui)
    }
  }
  
  def getRdds( appId: String): AllRDDResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new AllRDDResource(ui)
    }
  }
  
  def getRdds(
     appId: String,
      attemptId: String): AllRDDResource = {
    uiRoot.withSparkUI(appId, Some(attemptId)) { ui =>
      new AllRDDResource(ui)
    }
  }
  
  def getRdd( appId: String): OneRDDResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new OneRDDResource(ui)
    }
  }
  
  def getRdd(
     appId: String,
       attemptId: String): OneRDDResource = {
    uiRoot.withSparkUI(appId, Some(attemptId)) { ui =>
      new OneRDDResource(ui)
    }
  }
  
  def getEventLogs(
      appId: String): EventLogDownloadResource = {
    new EventLogDownloadResource(uiRoot, appId, None)
  }
  
  def getEventLogs(
     appId: String,
     attemptId: String): EventLogDownloadResource = {
    new EventLogDownloadResource(uiRoot, appId, Some(attemptId))
  }
  
  def getVersion(): VersionResource = {
    new VersionResource(uiRoot)
  }

}

private[spark] object ApiRootResource {

  def getServletHandler(uiRoot: UIRoot): ServletContextHandler = {
    val jerseyContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    jerseyContext.setContextPath("/api")
    UIRootFromServletContext.setUiRoot(jerseyContext, uiRoot)
    jerseyContext
  }
}

/**
 * This trait is shared by the all the root containers for application UI information --
 * the HistoryServer and the application UI.  This provides the common
 * interface needed for them all to expose application info as json.
 */
private[spark] trait UIRoot {
  def getSparkUI(appKey: String): Option[SparkUI]
  def getApplicationInfoList: Iterator[ApplicationInfo]

  /**
   * Write the event logs for the given app to the [[ZipOutputStream]] instance. If attemptId is
   * [[None]], event logs for all attempts of this application will be written out.
   */
  def writeEventLogs(appId: String, attemptId: Option[String], zipStream: ZipOutputStream): Unit = {
  }

  /**
   * Get the spark UI with the given appID, and apply a function
   * to it.  If there is no such app, throw an appropriate exception
   */
  def withSparkUI[T](appId: String, attemptId: Option[String])(f: SparkUI => T): T = {
    val appKey = attemptId.map(appId + "/" + _).getOrElse(appId)
    getSparkUI(appKey) match {
      case Some(ui) =>
        f(ui)
    }
  }
  def securityManager: SecurityManager
}

private[v1] object UIRootFromServletContext {

  private val attribute = getClass.getCanonicalName

  def setUiRoot(contextHandler: ContextHandler, uiRoot: UIRoot): Unit = {
    contextHandler.setAttribute(attribute, uiRoot)
  }

  def getUiRoot(context: ServletContext): UIRoot = {
    context.getAttribute(attribute).asInstanceOf[UIRoot]
  }
}

private[v1] trait UIRootFromServletContext {
  var servletContext: ServletContext = _

  def uiRoot: UIRoot = UIRootFromServletContext.getUiRoot(servletContext)
}

/**
 * Signal to JacksonMessageWriter to not convert the message into json (which would result in an
 * extra set of quotes).
 */
private[v1] case class ErrorWrapper(s: String)
