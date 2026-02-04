import asyncio
import logging
import json
import shutil
import os
from typing import Any, List, Optional
import subprocess
from asyncio import subprocess as asyncio_subprocess

logger = logging.getLogger(__name__)

class NodeJSRuntime:
    """
    A runtime for executing JavaScript code using an external Node.js process.
    Replicates the functionality of the Rust js-v8 crate but using Node.js instead of embedded V8.
    """
    def __init__(self):
        self.node_path = shutil.which("node")
        if not self.node_path:
            logger.warning("Node.js not found in PATH. JS runtime will not function.")

    async def eval(self, script: str) -> str:
        """
        Evaluates a JS script and returns the result as a string.
        """
        if not self.node_path:
            raise RuntimeError("Node.js not available")

        # Wrapper to print result
        wrapped_script = f"""
        try {{
            const result = eval({json.dumps(script)});
            if (typeof result === 'object') {{
                console.log(JSON.stringify(result));
            }} else {{
                console.log(result);
            }}
        }} catch (e) {{
            console.error(e);
            process.exit(1);
        }}
        """
        
        proc = await asyncio.create_subprocess_exec(
            self.node_path, "-e", wrapped_script,
            stdout=asyncio_subprocess.PIPE,
            stderr=asyncio_subprocess.PIPE
        )
        
        stdout, stderr = await proc.communicate()
        
        if proc.returncode != 0:
            error_msg = stderr.decode().strip()
            raise RuntimeError(f"JS Execution Error: {error_msg}")
            
        return stdout.decode().strip()

    async def call_function(self, script_path: str, func_name: str, args: List[Any]) -> Any:
        """
        Loads a script file and calls a specific function with arguments.
        """
        if not self.node_path:
            raise RuntimeError("Node.js not available")

        if not os.path.exists(script_path):
             raise FileNotFoundError(f"JS file not found: {script_path}")

        # Create a small harness to load the file and call the function
        # We assume the file exports the function or defines it globally.
        # If it's a module, we might need require().
        
        # Simple harness for global function or CommonJS export
        # We escape backslashes for Windows paths
        script_path_escaped = script_path.replace("\\", "\\\\")
        
        harness = f"""
        const path = require('path');
        // const scriptPath = path.resolve('{script_path_escaped}');
        // Require absolute path directly
        const scriptPath = '{script_path_escaped}';
        
        try {{
            const module = require(scriptPath);
            // Resolve function from dot notation (e.g. "steps.0.generate")
            const parts = '{func_name}'.split('.');
            let func = module;
            let context = null;

            for (const part of parts) {{
                if (func === undefined || func === null) break;
                context = func;
                func = func[part];
            }}

            if (typeof func !== 'function') {{
                // Fallback: try global
                if (parts.length === 1) {{
                    func = global['{func_name}'];
                    context = global;
                }}
            }}
            
            if (typeof func !== 'function') {{
                 // Try to see if the module itself is the function
                 if (parts.length === 1 && typeof module === 'function' && '{func_name}' === 'default') {{
                    func = module;
                    context = null;
                 }} else {{
                     throw new Error(`Function {func_name} not found in ${{scriptPath}}`);
                 }}
            }}
            
            const args = {json.dumps(args)};
            
            // Bind context if available
            if (context) {{
                func = func.bind(context);
            }}
            
            Promise.resolve(func(...args)).then(result => {{
                if (typeof result === 'object') {{
                     console.log(JSON.dumps(result));
                }} else {{
                     console.log(result);
                }}
            }}).catch(err => {{
                console.error(err);
                process.exit(1);
            }});
        }} catch (e) {{
            console.error(e);
            process.exit(1);
        }}
        """
        
        proc = await asyncio.create_subprocess_exec(
            self.node_path, "-e", harness,
            stdout=asyncio_subprocess.PIPE,
            stderr=asyncio_subprocess.PIPE
        )
        
        stdout, stderr = await proc.communicate()
        
        if proc.returncode != 0:
            error_msg = stderr.decode().strip()
            raise RuntimeError(f"JS Function Call Error: {error_msg}")
            
        output = stdout.decode().strip()
        try:
            return json.loads(output)
        except json.JSONDecodeError:
            return output
