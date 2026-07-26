import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, extname, join, relative, resolve } from "node:path";

import ts from "typescript";

type ImportBinding = {
  readonly moduleSpecifier: string;
  readonly importedName: string | null;
  readonly namespace: boolean;
};

export type CommunitySourceInventory = {
  readonly publicRoutes: ReadonlySet<string>;
  readonly pageRoutes: ReadonlySet<string>;
  readonly trpcPaths: ReadonlySet<string>;
  readonly mcpFeatures: ReadonlySet<string>;
  readonly mcpTools: ReadonlySet<string>;
  readonly mcpToolsByFeature: ReadonlyMap<string, ReadonlySet<string>>;
  readonly queueNames: ReadonlySet<string>;
  readonly batchTableNames: ReadonlySet<string>;
  readonly workerRoutes: ReadonlySet<string>;
  readonly workerRegistrations: ReadonlySet<string>;
};

const EXECUTABLE_EVIDENCE_KINDS = new Set([
  "public",
  "page",
  "trpc",
  "mcp",
  "queue",
  "worker",
]);

function findRepositoryRoot(start: string): string {
  let candidate = resolve(start);
  while (dirname(candidate) !== candidate) {
    if (existsSync(join(candidate, "pnpm-workspace.yaml"))) return candidate;
    candidate = dirname(candidate);
  }
  throw new Error(`Unable to find repository root from ${start}`);
}

function walkSourceFiles(root: string): string[] {
  if (!existsSync(root)) return [];

  return readdirSync(root, { withFileTypes: true }).flatMap((entry) => {
    const path = join(root, entry.name);
    if (entry.isDirectory()) return walkSourceFiles(path);
    if (!entry.isFile() || !/\.[cm]?[jt]sx?$/.test(entry.name)) return [];
    if (/\.(?:test|servertest|clienttest)\.[cm]?[jt]sx?$/.test(entry.name)) {
      return [];
    }
    return [path];
  });
}

function pageRouteFromFile(file: string, pagesRoot: string): string {
  const withoutExtension = relative(pagesRoot, file).slice(
    0,
    -extname(file).length,
  );
  const withoutIndex = withoutExtension.replace(/(?:^|\/)index$/, "");
  return `/${withoutIndex}`.replace(/\/$/, "") || "/";
}

function createSourceFile(path: string): ts.SourceFile {
  return ts.createSourceFile(
    path,
    readFileSync(path, "utf8"),
    ts.ScriptTarget.Latest,
    true,
    path.endsWith("x") ? ts.ScriptKind.TSX : ts.ScriptKind.TS,
  );
}

function importBindings(sourceFile: ts.SourceFile): Map<string, ImportBinding> {
  const bindings = new Map<string, ImportBinding>();
  for (const statement of sourceFile.statements) {
    if (
      !ts.isImportDeclaration(statement) ||
      !ts.isStringLiteral(statement.moduleSpecifier) ||
      !statement.importClause
    ) {
      continue;
    }

    const moduleSpecifier = statement.moduleSpecifier.text;
    if (statement.importClause.name) {
      bindings.set(statement.importClause.name.text, {
        moduleSpecifier,
        importedName: "default",
        namespace: false,
      });
    }

    const namedBindings = statement.importClause.namedBindings;
    if (namedBindings && ts.isNamespaceImport(namedBindings)) {
      bindings.set(namedBindings.name.text, {
        moduleSpecifier,
        importedName: null,
        namespace: true,
      });
    } else if (namedBindings) {
      for (const element of namedBindings.elements) {
        bindings.set(element.name.text, {
          moduleSpecifier,
          importedName: element.propertyName?.text ?? element.name.text,
          namespace: false,
        });
      }
    }
  }
  return bindings;
}

function resolveLocalModule(
  repositoryRoot: string,
  importer: string,
  moduleSpecifier: string,
): string | null {
  let base: string;
  if (moduleSpecifier.startsWith("@/src/")) {
    base = join(
      repositoryRoot,
      "web/src",
      moduleSpecifier.slice("@/src/".length),
    );
  } else if (moduleSpecifier.startsWith(".")) {
    base = resolve(dirname(importer), moduleSpecifier);
  } else {
    return null;
  }

  const candidates = [
    base,
    `${base}.ts`,
    `${base}.tsx`,
    join(base, "index.ts"),
    join(base, "index.tsx"),
  ];
  return (
    candidates.find(
      (candidate) => existsSync(candidate) && statSync(candidate).isFile(),
    ) ?? null
  );
}

function propertyName(property: ts.ObjectLiteralElementLike): string | null {
  if (
    !ts.isPropertyAssignment(property) &&
    !ts.isShorthandPropertyAssignment(property) &&
    !ts.isMethodDeclaration(property)
  ) {
    return null;
  }
  if (ts.isIdentifier(property.name) || ts.isStringLiteral(property.name)) {
    return property.name.text;
  }
  return null;
}

function findVariableInitializer(
  sourceFile: ts.SourceFile,
  bindingName: string,
): ts.Expression | null {
  for (const statement of sourceFile.statements) {
    if (!ts.isVariableStatement(statement)) continue;
    for (const declaration of statement.declarationList.declarations) {
      if (ts.isIdentifier(declaration.name)) {
        if (declaration.name.text === bindingName) {
          return declaration.initializer ?? null;
        }
        continue;
      }
      if (
        ts.isArrayBindingPattern(declaration.name) &&
        declaration.name.elements.some(
          (element) =>
            ts.isBindingElement(element) &&
            ts.isIdentifier(element.name) &&
            element.name.text === bindingName,
        )
      ) {
        return declaration.initializer ?? null;
      }
    }
  }
  return null;
}

function routerObject(
  initializer: ts.Expression | null,
): ts.ObjectLiteralExpression | null {
  if (!initializer || !ts.isCallExpression(initializer)) return null;
  const [argument] = initializer.arguments;
  return argument && ts.isObjectLiteralExpression(argument) ? argument : null;
}

function unwrapExpression(expression: ts.Expression): ts.Expression {
  let unwrapped = expression;
  while (
    ts.isAsExpression(unwrapped) ||
    ts.isSatisfiesExpression(unwrapped) ||
    ts.isTypeAssertionExpression(unwrapped)
  ) {
    unwrapped = unwrapped.expression;
  }
  return unwrapped;
}

function collectTrpcPaths(
  repositoryRoot: string,
  file: string,
  routerBinding: string,
  prefix: string,
  result: Set<string>,
  visited: Set<string>,
): void {
  const visitKey = `${file}:${routerBinding}:${prefix}`;
  if (visited.has(visitKey)) return;
  visited.add(visitKey);

  const sourceFile = createSourceFile(file);
  const object = routerObject(
    findVariableInitializer(sourceFile, routerBinding),
  );
  if (!object) return;
  const imports = importBindings(sourceFile);

  for (const property of object.properties) {
    const name = propertyName(property);
    if (!name) continue;
    const path = prefix ? `${prefix}.${name}` : name;
    result.add(path);

    if (!ts.isPropertyAssignment(property)) continue;
    const nestedIdentifier = ts.isIdentifier(property.initializer)
      ? property.initializer.text
      : null;
    if (!nestedIdentifier) continue;
    const binding = imports.get(nestedIdentifier);
    if (!binding || binding.namespace || !binding.importedName) continue;
    const nestedFile = resolveLocalModule(
      repositoryRoot,
      file,
      binding.moduleSpecifier,
    );
    if (!nestedFile) continue;
    collectTrpcPaths(
      repositoryRoot,
      nestedFile,
      binding.importedName,
      path,
      result,
      visited,
    );
  }
}

function objectStringProperty(
  object: ts.ObjectLiteralExpression,
  targetName: string,
): string | null {
  const property = object.properties.find(
    (candidate) => propertyName(candidate) === targetName,
  );
  if (
    !property ||
    !ts.isPropertyAssignment(property) ||
    !ts.isStringLiteral(property.initializer)
  ) {
    return null;
  }
  return property.initializer.text;
}

function findFirstObjectLiteral(
  expression: ts.Expression,
): ts.ObjectLiteralExpression | null {
  if (ts.isObjectLiteralExpression(expression)) return expression;
  if (ts.isCallExpression(expression)) {
    for (const argument of expression.arguments) {
      const nested = findFirstObjectLiteral(argument);
      if (nested) return nested;
    }
  }
  return null;
}

function toolNameForReference(
  repositoryRoot: string,
  featureFile: string,
  reference: ts.Expression,
  imports: ReadonlyMap<string, ImportBinding>,
): string | null {
  let localName: string;
  let binding: ImportBinding | undefined;
  if (ts.isIdentifier(reference)) {
    localName = reference.text;
    binding = imports.get(localName);
  } else if (
    ts.isPropertyAccessExpression(reference) &&
    ts.isIdentifier(reference.expression)
  ) {
    localName = reference.name.text;
    const namespaceBinding = imports.get(reference.expression.text);
    binding = namespaceBinding?.namespace
      ? { ...namespaceBinding, importedName: localName, namespace: false }
      : undefined;
  } else {
    return null;
  }

  const definitionFile = binding
    ? resolveLocalModule(repositoryRoot, featureFile, binding.moduleSpecifier)
    : featureFile;
  const definitionName = binding?.importedName ?? localName;
  if (!definitionFile || !definitionName) return null;
  const initializer = findVariableInitializer(
    createSourceFile(definitionFile),
    definitionName,
  );
  const object = initializer ? findFirstObjectLiteral(initializer) : null;
  return object ? objectStringProperty(object, "name") : null;
}

function collectMcpInventory(repositoryRoot: string): {
  features: Set<string>;
  tools: Set<string>;
  toolsByFeature: Map<string, ReadonlySet<string>>;
} {
  const bootstrapFile = join(
    repositoryRoot,
    "web/src/features/mcp/server/bootstrap.ts",
  );
  const bootstrap = createSourceFile(bootstrapFile);
  const bootstrapImports = importBindings(bootstrap);
  const featureArray = findVariableInitializer(bootstrap, "MCP_FEATURES");
  const features = new Set<string>();
  const tools = new Set<string>();
  const toolsByFeature = new Map<string, ReadonlySet<string>>();
  if (!featureArray) {
    return { features, tools, toolsByFeature };
  }

  const arrayExpression = unwrapExpression(featureArray);
  if (!ts.isArrayLiteralExpression(arrayExpression)) {
    return { features, tools, toolsByFeature };
  }

  for (const featureReference of arrayExpression.elements) {
    if (!ts.isIdentifier(featureReference)) continue;
    const featureBinding = bootstrapImports.get(featureReference.text);
    if (!featureBinding?.importedName) continue;
    const featureFile = resolveLocalModule(
      repositoryRoot,
      bootstrapFile,
      featureBinding.moduleSpecifier,
    );
    if (!featureFile) continue;
    const featureSource = createSourceFile(featureFile);
    const featureInitializer = findVariableInitializer(
      featureSource,
      featureBinding.importedName,
    );
    if (!featureInitializer) {
      continue;
    }
    const featureObject = unwrapExpression(featureInitializer);
    if (!ts.isObjectLiteralExpression(featureObject)) continue;

    const featureName = objectStringProperty(featureObject, "name");
    if (!featureName) continue;
    features.add(featureName);
    const featureTools = new Set<string>();
    const toolsProperty = featureObject.properties.find(
      (property) => propertyName(property) === "tools",
    );
    if (
      !toolsProperty ||
      !ts.isPropertyAssignment(toolsProperty) ||
      !ts.isArrayLiteralExpression(toolsProperty.initializer)
    ) {
      toolsByFeature.set(featureName, featureTools);
      continue;
    }

    const featureImports = importBindings(featureSource);
    for (const toolEntry of toolsProperty.initializer.elements) {
      if (!ts.isObjectLiteralExpression(toolEntry)) continue;
      const definition = toolEntry.properties.find(
        (property) => propertyName(property) === "definition",
      );
      if (!definition || !ts.isPropertyAssignment(definition)) continue;
      const name = toolNameForReference(
        repositoryRoot,
        featureFile,
        definition.initializer,
        featureImports,
      );
      if (!name) continue;
      tools.add(name);
      featureTools.add(name);
    }
    toolsByFeature.set(featureName, featureTools);
  }

  return { features, tools, toolsByFeature };
}

function collectQueueNames(repositoryRoot: string): Set<string> {
  const sourceFile = createSourceFile(
    join(repositoryRoot, "packages/shared/src/server/queues.ts"),
  );
  const names = new Set<string>();
  for (const statement of sourceFile.statements) {
    if (
      !ts.isEnumDeclaration(statement) ||
      statement.name.text !== "QueueName"
    ) {
      continue;
    }
    for (const member of statement.members) {
      if (member.initializer && ts.isStringLiteral(member.initializer)) {
        names.add(member.initializer.text);
      }
    }
  }
  return names;
}

function collectBatchTableNames(repositoryRoot: string): Set<string> {
  const sourceFile = createSourceFile(
    join(repositoryRoot, "packages/shared/src/interfaces/tableNames.ts"),
  );
  const names = new Set<string>();
  for (const statement of sourceFile.statements) {
    if (
      !ts.isEnumDeclaration(statement) ||
      statement.name.text !== "BatchTableNames"
    ) {
      continue;
    }
    for (const member of statement.members) {
      if (member.initializer && ts.isStringLiteral(member.initializer)) {
        names.add(member.initializer.text);
      }
    }
  }
  return names;
}

function collectWorkerInventory(repositoryRoot: string): {
  routes: Set<string>;
  registrations: Set<string>;
} {
  const routes = new Set<string>();
  const appSource = readFileSync(
    join(repositoryRoot, "worker/src/app.ts"),
    "utf8",
  );
  const apiSource = readFileSync(
    join(repositoryRoot, "worker/src/api/index.ts"),
    "utf8",
  );
  const mount = appSource.match(
    /app\.use\(\s*["']([^"']+)["']\s*,\s*api\s*\)/,
  )?.[1];
  if (mount) {
    for (const match of apiSource.matchAll(
      /router\.(?:get|post|put|patch|delete)\s*(?:<[^;]+?>)?\s*\(\s*["']([^"']+)["']/gs,
    )) {
      routes.add(`${mount}${match[1]}`.replace(/\/{2,}/g, "/"));
    }
  }
  const registrations = new Set(
    [...appSource.matchAll(/\b([A-Za-z_$][\w$]*)\.getInstance\s*\(/g)].map(
      (match) => match[1]!,
    ),
  );
  return { routes, registrations };
}

let cachedInventory: CommunitySourceInventory | null = null;

export function collectCommunitySourceInventory(): CommunitySourceInventory {
  if (cachedInventory) return cachedInventory;
  const repositoryRoot = findRepositoryRoot(process.cwd());
  const pagesRoot = join(repositoryRoot, "web/src/pages");
  const publicRoutes = new Set(
    walkSourceFiles(join(pagesRoot, "api/public")).map((file) =>
      pageRouteFromFile(file, pagesRoot),
    ),
  );
  const pageRoutes = new Set(
    walkSourceFiles(join(pagesRoot, "project")).map((file) =>
      pageRouteFromFile(file, pagesRoot),
    ),
  );
  const trpcPaths = new Set<string>();
  collectTrpcPaths(
    repositoryRoot,
    join(repositoryRoot, "web/src/server/api/root.ts"),
    "appRouter",
    "",
    trpcPaths,
    new Set(),
  );
  const mcp = collectMcpInventory(repositoryRoot);
  const worker = collectWorkerInventory(repositoryRoot);

  cachedInventory = {
    publicRoutes,
    pageRoutes,
    trpcPaths,
    mcpFeatures: mcp.features,
    mcpTools: mcp.tools,
    mcpToolsByFeature: mcp.toolsByFeature,
    queueNames: collectQueueNames(repositoryRoot),
    batchTableNames: collectBatchTableNames(repositoryRoot),
    workerRoutes: worker.routes,
    workerRegistrations: worker.registrations,
  };
  return cachedInventory;
}

function hasRouteOrDescendant(
  routes: ReadonlySet<string>,
  target: string,
): boolean {
  return [...routes].some(
    (route) => route === target || route.startsWith(`${target}/`),
  );
}

export function isExecutableEvidence(evidence: string): boolean {
  return EXECUTABLE_EVIDENCE_KINDS.has(evidence.split(":", 1)[0] ?? "");
}

export function executableEvidenceExists(
  evidence: string,
  inventory: CommunitySourceInventory,
): boolean {
  const separator = evidence.indexOf(":");
  if (separator < 0) return false;
  const kind = evidence.slice(0, separator);
  const target = evidence.slice(separator + 1);
  switch (kind) {
    case "public":
      return hasRouteOrDescendant(inventory.publicRoutes, target);
    case "page":
      return hasRouteOrDescendant(inventory.pageRoutes, target);
    case "trpc": {
      const [trpcPath, ...tableNames] = target.split(":");
      return (
        Boolean(trpcPath && inventory.trpcPaths.has(trpcPath)) &&
        tableNames.every((tableName) =>
          inventory.batchTableNames.has(tableName),
        )
      );
    }
    case "mcp":
      return (
        inventory.mcpFeatures.has(target) || inventory.mcpTools.has(target)
      );
    case "queue":
      return inventory.queueNames.has(target);
    case "worker":
      return target.startsWith("/")
        ? inventory.workerRoutes.has(target)
        : inventory.workerRegistrations.has(target);
    default:
      return false;
  }
}

export function missingExecutableEvidence(
  evidence: readonly string[],
  inventory: CommunitySourceInventory,
): string[] {
  return evidence
    .filter(isExecutableEvidence)
    .filter((entry) => !executableEvidenceExists(entry, inventory))
    .sort();
}
